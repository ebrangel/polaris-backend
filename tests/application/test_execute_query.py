"""`ExecuteQuery` — o fluxo completo da seção 3: validar, autorizar, aplicar o teto de
`limit`, checar o cache, resolver o dataset, estimar custo (enfileirar se pesada —
seção 2.4), executar, cachear.
"""

import dataclasses
import logging

import pytest
from fixtures import catalog, eventos_schema, vendas_schema, vendas_schema_com_canal

from application.ports.query_executor import QueryCost
from application.use_cases import ExecuteQuery, ResolveDataset
from domain.errors import (
    ForbiddenMeasureError,
    InvalidFilterError,
    NoDatasetAvailableError,
    QueryTimeoutError,
    RateLimitedError,
    UnknownFieldError,
    UnknownSchemaError,
)
from domain.models import (
    Catalog,
    Filter,
    FilterOperator,
    QueryRequest,
    QueryResult,
    QueryStatus,
)
from fakes import InMemoryCacheGateway, InMemoryJobQueue, InMemoryRateLimiter, StubQueryExecutor


def make_execute_query(
    cat,
    *,
    executors=None,
    cache=None,
    job_queue=None,
    request_rate_limiter=None,
    heavy_query_rate_limiter=None,
    max_heavy_queue_depth=None,
    slow_query_threshold_ms=None,
):
    return ExecuteQuery(
        catalog=cat,
        resolve_dataset=ResolveDataset(),
        executors=executors if executors is not None else {},
        cache=cache if cache is not None else InMemoryCacheGateway(),
        job_queue=job_queue if job_queue is not None else InMemoryJobQueue(),
        request_rate_limiter=request_rate_limiter,
        heavy_query_rate_limiter=heavy_query_rate_limiter,
        max_heavy_queue_depth=max_heavy_queue_depth,
        slow_query_threshold_ms=slow_query_threshold_ms,
    )


# --- Caminho feliz -----------------------------------------------------------------------


async def test_caminho_feliz_da_secao_2_2_devolve_o_formato_da_secao_2_3():
    schema = vendas_schema()
    stub = StubQueryExecutor()
    execute = make_execute_query(
        Catalog(schemas={"vendas": schema}), executors={"env:DW_VENDAS_PG_URL": stub}
    )
    request = QueryRequest(
        schema="vendas",
        dimensions=("sigla_uf",),
        measures=("valor_total", "quantidade"),
        filters=(Filter(field="sigla_uf", operator=FilterOperator.IN, value=["SP", "RJ"]),),
        order_by=(),
    )

    result = await execute(request, roles=["financeiro"], client_id="cliente-1")

    assert result.status is QueryStatus.COMPLETED
    assert result.meta.dataset_used == "vendas_agregado_uf"
    assert [(c.field, c.type.value, c.format) for c in result.columns] == [
        ("sigla_uf", "string", None),
        ("valor_total", "number", "currency"),
        ("quantidade", "number", None),
    ]


async def test_eventos_navegacao_resolve_para_o_dataset_elasticsearch():
    stub = StubQueryExecutor()
    execute = make_execute_query(
        Catalog(schemas={"eventos_navegacao": eventos_schema()}),
        executors={"env:ES_EVENTOS_URL": stub},
    )
    request = QueryRequest(
        schema="eventos_navegacao", dimensions=("pais",), measures=("duracao_media",)
    )

    result = await execute(request, roles=[], client_id="cliente-1")

    assert result.status is QueryStatus.COMPLETED
    assert len(stub.calls) == 1


# --- Cache pelo query_id -------------------------------------------------------------------


async def test_cache_miss_grava_e_cache_hit_nao_executa_de_novo():
    stub = StubQueryExecutor()
    cache = InMemoryCacheGateway()
    execute = make_execute_query(
        Catalog(schemas={"vendas": vendas_schema()}),
        executors={"env:DW_VENDAS_PG_URL": stub},
        cache=cache,
    )
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))

    first = await execute(request, roles=["financeiro"], client_id="cliente-1")
    assert len(stub.calls) == 1
    assert first.meta.cached is False
    assert await cache.get(request.query_id) == first

    second = await execute(request, roles=["financeiro"], client_id="cliente-1")

    assert len(stub.calls) == 1  # não chamou o executor de novo
    assert second.meta.cached is True
    assert second.meta.execution_ms == first.meta.execution_ms
    assert second.meta.dataset_used == first.meta.dataset_used
    assert second.rows == first.rows


async def test_resultado_failed_nao_e_cacheado():
    schema = vendas_schema()
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",))
    stub = StubQueryExecutor(result=QueryResult.failed(request.query_id, error="query_timeout"))
    cache = InMemoryCacheGateway()
    execute = make_execute_query(
        Catalog(schemas={"vendas": schema}),
        executors={"env:DW_VENDAS_PG_URL": stub},
        cache=cache,
    )

    result = await execute(request, roles=[], client_id="cliente-1")

    assert result.status is QueryStatus.FAILED
    assert await cache.get(request.query_id) is None


async def test_erro_do_executor_propaga_e_nao_e_cacheado():
    schema = vendas_schema()
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",))
    stub = StubQueryExecutor(raises=QueryTimeoutError("estourou o timeout"))
    cache = InMemoryCacheGateway()
    execute = make_execute_query(
        Catalog(schemas={"vendas": schema}),
        executors={"env:DW_VENDAS_PG_URL": stub},
        cache=cache,
    )

    with pytest.raises(QueryTimeoutError):
        await execute(request, roles=[], client_id="cliente-1")

    assert await cache.get(request.query_id) is None


# --- Roteamento por datasource ---------------------------------------------------------


async def test_roteamento_por_datasource_segue_o_dataset_resolvido():
    """`sigla_uf` sozinho resolve para `vendas_agregado_uf` (Postgres); acrescentar
    `cargo` resolve para `vendas_detalhado` (Oracle) — seção 2.2."""
    postgres_stub = StubQueryExecutor()
    oracle_stub = StubQueryExecutor()
    execute = make_execute_query(
        Catalog(schemas={"vendas": vendas_schema()}),
        executors={
            "env:DW_VENDAS_PG_URL": postgres_stub,
            "env:DW_VENDAS_ORACLE_URL": oracle_stub,
        },
    )

    only_uf = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))
    await execute(only_uf, roles=["financeiro"], client_id="cliente-1")
    assert len(postgres_stub.calls) == 1
    assert len(oracle_stub.calls) == 0

    with_cargo = QueryRequest(
        schema="vendas", dimensions=("sigla_uf", "cargo"), measures=("valor_total",)
    )
    await execute(with_cargo, roles=["financeiro"], client_id="cliente-1")
    assert len(oracle_stub.calls) == 1


async def test_executor_nao_configurado_levanta_lookup_error():
    execute = make_execute_query(Catalog(schemas={"vendas": vendas_schema()}), executors={})
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",))

    with pytest.raises(LookupError, match="vendas_agregado_uf"):
        await execute(request, roles=[], client_id="cliente-1")


# --- Custo estimado e fila (seção 2.4) --------------------------------------------------


async def test_consulta_pesada_enfileira_sem_executar_nem_cachear():
    heavy_cost = QueryCost(score=100, threshold=50)
    stub = StubQueryExecutor(cost=heavy_cost)
    cache = InMemoryCacheGateway()
    job_queue = InMemoryJobQueue()
    execute = make_execute_query(
        Catalog(schemas={"vendas": vendas_schema()}),
        executors={"env:DW_VENDAS_PG_URL": stub},
        cache=cache,
        job_queue=job_queue,
    )
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))

    result = await execute(request, roles=["financeiro"], client_id="cliente-1")

    assert result.status is QueryStatus.PROCESSING
    assert result.query_id == request.query_id
    assert stub.calls == []  # `execute()` nunca foi chamado
    assert await cache.get(request.query_id) is None  # nada cacheado
    assert job_queue.calls == [(request, "vendas_agregado_uf")]  # dataset.name correto


async def test_consulta_leve_nao_enfileira():
    light_cost = QueryCost(score=1, threshold=50)
    job_queue = InMemoryJobQueue()
    execute = make_execute_query(
        Catalog(schemas={"vendas": vendas_schema()}),
        executors={"env:DW_VENDAS_PG_URL": StubQueryExecutor(cost=light_cost)},
        job_queue=job_queue,
    )
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))

    result = await execute(request, roles=["financeiro"], client_id="cliente-1")

    assert result.status is QueryStatus.COMPLETED
    assert job_queue.calls == []


async def test_cache_hit_nao_chega_a_estimar_custo():
    """A ordem importa (seção 2.4: "depois de resolver o dataset, estimar custo"): o
    cache é checado antes, então um acerto não deveria nem chegar lá. Prova indireta:
    se a estimativa rodasse no segundo `execute`, o `cost` (mutado para pesado depois
    do primeiro request) faria a consulta ser enfileirada em vez de vir do cache."""
    stub = StubQueryExecutor()
    cache = InMemoryCacheGateway()
    job_queue = InMemoryJobQueue()
    execute = make_execute_query(
        Catalog(schemas={"vendas": vendas_schema()}),
        executors={"env:DW_VENDAS_PG_URL": stub},
        cache=cache,
        job_queue=job_queue,
    )
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))

    first = await execute(request, roles=["financeiro"], client_id="cliente-1")
    assert first.status is QueryStatus.COMPLETED

    stub.cost = QueryCost(score=100, threshold=50)  # a partir daqui, "pesada"
    second = await execute(request, roles=["financeiro"], client_id="cliente-1")

    assert second.status is QueryStatus.COMPLETED
    assert second.meta.cached is True
    assert job_queue.calls == []


# --- Teto de limit por schema (seção 2.6) -----------------------------------------------


async def test_teto_de_limit_por_schema_e_aplicado_e_compartilha_cache():
    schema = dataclasses.replace(vendas_schema(), max_limit=500)
    stub = StubQueryExecutor()
    cache = InMemoryCacheGateway()
    execute = make_execute_query(
        Catalog(schemas={"vendas": schema}),
        executors={"env:DW_VENDAS_PG_URL": stub},
        cache=cache,
    )

    over_limit = QueryRequest(schema="vendas", dimensions=("sigla_uf",), limit=10_000)
    await execute(over_limit, roles=[], client_id="cliente-1")

    executed_request = stub.calls[0][1]
    assert executed_request.limit == 500

    at_limit = QueryRequest(schema="vendas", dimensions=("sigla_uf",), limit=500)
    result = await execute(at_limit, roles=[], client_id="cliente-1")

    assert len(stub.calls) == 1  # mesma entrada de cache, não executou de novo
    assert result.meta.cached is True


# --- Os erros da seção 2.5, todos abortando antes do executor --------------------------


async def test_unknown_schema():
    execute = make_execute_query(catalog())
    request = QueryRequest(schema="produtos", dimensions=("nome",))

    with pytest.raises(UnknownSchemaError):
        await execute(request, roles=[], client_id="cliente-1")


async def test_unknown_field():
    execute = make_execute_query(catalog())
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf", "canal"))

    with pytest.raises(UnknownFieldError):
        await execute(request, roles=[], client_id="cliente-1")


async def test_invalid_filter():
    """`between` não é válido para dimensões `string` (seção 2.2)."""
    execute = make_execute_query(catalog())
    request = QueryRequest(
        schema="vendas",
        dimensions=("sigla_uf",),
        filters=(Filter(field="sigla_uf", operator=FilterOperator.BETWEEN, value=["A", "Z"]),),
    )

    with pytest.raises(InvalidFilterError):
        await execute(request, roles=[], client_id="cliente-1")


async def test_forbidden_measure():
    execute = make_execute_query(catalog())
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))

    with pytest.raises(ForbiddenMeasureError):
        await execute(request, roles=["comercial"], client_id="cliente-1")


async def test_no_dataset_available():
    schema = vendas_schema_com_canal()
    execute = make_execute_query(Catalog(schemas={"vendas": schema}))
    request = QueryRequest(
        schema="vendas",
        dimensions=("sigla_uf", "cargo", "canal"),
        measures=("valor_total",),
    )

    with pytest.raises(NoDatasetAvailableError):
        await execute(request, roles=["financeiro"], client_id="cliente-1")


# --- roles/client_id são obrigatórios -----------------------------------------------------


async def test_roles_e_keyword_only_obrigatorio():
    execute = make_execute_query(catalog())
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",))

    with pytest.raises(TypeError):
        await execute(request)


async def test_client_id_e_keyword_only_obrigatorio():
    """Mesma convenção de `roles` (Marco 9): sem default silencioso — quem chama o use
    case precisa decidir explicitamente qual cliente está fazendo a requisição."""
    execute = make_execute_query(catalog())
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",))

    with pytest.raises(TypeError):
        await execute(request, roles=[])


# --- Rate limiting por cliente (Marco 9) --------------------------------------------------


async def test_sem_rate_limiter_configurado_nada_e_limitado():
    """Regressão: construir `ExecuteQuery` sem os parâmetros do Marco 9 continua se
    comportando exatamente como nos Marcos 4-8."""
    stub = StubQueryExecutor()
    execute = make_execute_query(
        Catalog(schemas={"vendas": vendas_schema()}),
        executors={"env:DW_VENDAS_PG_URL": stub},
    )
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))

    for _ in range(10):
        result = await execute(request, roles=["financeiro"], client_id="cliente-1")
        assert result.status is QueryStatus.COMPLETED


async def test_limite_geral_excedido_levanta_rate_limited_sem_executar():
    stub = StubQueryExecutor()
    limiter = InMemoryRateLimiter(limit=2)
    execute = make_execute_query(
        Catalog(schemas={"vendas": vendas_schema()}),
        executors={"env:DW_VENDAS_PG_URL": stub},
        request_rate_limiter=limiter,
    )
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))

    await execute(request, roles=["financeiro"], client_id="cliente-1")
    await execute(request, roles=["financeiro"], client_id="cliente-1")

    with pytest.raises(RateLimitedError):
        await execute(request, roles=["financeiro"], client_id="cliente-1")

    assert len(stub.calls) == 1  # 1a: cache miss, executa; 2a: cache hit; 3a: nem chega lá


async def test_limite_geral_e_por_cliente_independente():
    stub = StubQueryExecutor()
    limiter = InMemoryRateLimiter(limit=1)
    execute = make_execute_query(
        Catalog(schemas={"vendas": vendas_schema()}),
        executors={"env:DW_VENDAS_PG_URL": stub},
        request_rate_limiter=limiter,
    )
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))

    await execute(request, roles=["financeiro"], client_id="cliente-a")
    await execute(request, roles=["financeiro"], client_id="cliente-b")  # outro cliente, outro teto

    with pytest.raises(RateLimitedError):
        await execute(request, roles=["financeiro"], client_id="cliente-a")


async def test_limite_de_consultas_pesadas_excedido_levanta_rate_limited():
    heavy_cost = QueryCost(score=100, threshold=50)
    stub = StubQueryExecutor(cost=heavy_cost)
    job_queue = InMemoryJobQueue()
    limiter = InMemoryRateLimiter(limit=1)
    execute = make_execute_query(
        Catalog(schemas={"vendas": vendas_schema()}),
        executors={"env:DW_VENDAS_PG_URL": stub},
        job_queue=job_queue,
        heavy_query_rate_limiter=limiter,
    )
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))

    first = await execute(request, roles=["financeiro"], client_id="cliente-1")
    assert first.status is QueryStatus.PROCESSING

    # Mesma requisição não serve para o segundo teste (cache/mesmo query_id) — muda o
    # filtro para gerar um `query_id` novo e continuar caindo no branch pesado.
    second_request = dataclasses.replace(
        request,
        filters=(Filter(field="sigla_uf", operator=FilterOperator.EQ, value="SP"),),
    )
    with pytest.raises(RateLimitedError):
        await execute(second_request, roles=["financeiro"], client_id="cliente-1")

    assert len(job_queue.calls) == 1  # só o primeiro chegou a enfileirar


async def test_limite_de_consultas_pesadas_nao_afeta_consulta_leve():
    limiter = InMemoryRateLimiter(limit=0)  # nunca permite nada pesado
    light_cost = QueryCost(score=1, threshold=50)
    execute = make_execute_query(
        Catalog(schemas={"vendas": vendas_schema()}),
        executors={"env:DW_VENDAS_PG_URL": StubQueryExecutor(cost=light_cost)},
        heavy_query_rate_limiter=limiter,
    )
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))

    result = await execute(request, roles=["financeiro"], client_id="cliente-1")

    assert result.status is QueryStatus.COMPLETED
    assert limiter.calls == []  # o limite pesado nem é consultado


async def test_fila_no_teto_recusa_mesmo_com_limite_por_cliente_livre():
    """Backpressure global (`docs/escalabilidade.md`) — não depende de `client_id`,
    então é checado antes do `heavy_query_rate_limiter` e nem o consulta."""
    heavy_cost = QueryCost(score=100, threshold=50)
    stub = StubQueryExecutor(cost=heavy_cost)
    job_queue = InMemoryJobQueue()
    already_queued = QueryRequest(schema="vendas", dimensions=("cargo",))
    await job_queue.enqueue(already_queued, "vendas_detalhado")  # fila com profundidade 1
    heavy_limiter = InMemoryRateLimiter(limit=1_000)  # bem folgado — não é o motivo da recusa
    execute = make_execute_query(
        Catalog(schemas={"vendas": vendas_schema()}),
        executors={"env:DW_VENDAS_PG_URL": stub},
        job_queue=job_queue,
        heavy_query_rate_limiter=heavy_limiter,
        max_heavy_queue_depth=1,
    )
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))

    with pytest.raises(RateLimitedError):
        await execute(request, roles=["financeiro"], client_id="cliente-1")

    assert heavy_limiter.calls == []
    assert len(job_queue.calls) == 1  # só o pré-semeado — nada novo foi enfileirado


# --- Log de consultas lentas (Marco 9) ------------------------------------------------------


async def test_consulta_lenta_gera_log_de_warning(caplog):
    slow_result = QueryResult.completed(
        query_id="q_lento1",
        columns=(),
        rows=(),
        dataset_used="vendas_agregado_uf",
        execution_ms=5000,
    )
    stub = StubQueryExecutor(result=slow_result)
    execute = make_execute_query(
        Catalog(schemas={"vendas": vendas_schema()}),
        executors={"env:DW_VENDAS_PG_URL": stub},
        slow_query_threshold_ms=1000,
    )
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))

    with caplog.at_level(logging.WARNING):
        await execute(request, roles=["financeiro"], client_id="cliente-1")

    assert any("consulta lenta" in record.message for record in caplog.records)
    assert any("q_lento1" in record.message for record in caplog.records)


async def test_consulta_rapida_nao_gera_log(caplog):
    fast_result = QueryResult.completed(
        query_id="q_rapido1",
        columns=(),
        rows=(),
        dataset_used="vendas_agregado_uf",
        execution_ms=10,
    )
    stub = StubQueryExecutor(result=fast_result)
    execute = make_execute_query(
        Catalog(schemas={"vendas": vendas_schema()}),
        executors={"env:DW_VENDAS_PG_URL": stub},
        slow_query_threshold_ms=1000,
    )
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))

    with caplog.at_level(logging.WARNING):
        await execute(request, roles=["financeiro"], client_id="cliente-1")

    assert caplog.records == []


async def test_sem_threshold_configurado_nunca_loga(caplog):
    slow_result = QueryResult.completed(
        query_id="q_lento2",
        columns=(),
        rows=(),
        dataset_used="vendas_agregado_uf",
        execution_ms=999_999,
    )
    stub = StubQueryExecutor(result=slow_result)
    execute = make_execute_query(
        Catalog(schemas={"vendas": vendas_schema()}),
        executors={"env:DW_VENDAS_PG_URL": stub},
    )
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))

    with caplog.at_level(logging.WARNING):
        await execute(request, roles=["financeiro"], client_id="cliente-1")

    assert caplog.records == []
