"""`ExecuteQuery` — o fluxo da seção 3: validar, autorizar, aplicar o teto de `limit`,
checar o cache, resolver o dataset, backpressure de fila, enfileirar e aguardar o job
por uma janela curta (`inline_wait_seconds`).

Não há mais caminho síncrono nem estimativa de custo: toda consulta é enfileirada, e
quem executa e grava no cache é o worker (`RunQueuedQuery`, testado à parte). Aqui os
desfechos do job são simulados pelo `InMemoryJobQueue`.
"""

import dataclasses
import logging

import pytest
from fixtures import catalog, eventos_schema, vendas_schema, vendas_schema_com_canal

from application.use_cases import ExecuteQuery, ResolveDataset
from domain.errors import (
    ForbiddenMeasureError,
    InvalidFilterError,
    NoDatasetAvailableError,
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
from fakes import InMemoryCacheGateway, InMemoryJobQueue, InMemoryRateLimiter


def make_execute_query(
    cat,
    *,
    cache=None,
    job_queue=None,
    request_rate_limiter=None,
    max_queue_depth=None,
    default_max_limit=None,
    inline_wait_seconds=2.0,
):
    return ExecuteQuery(
        catalog=cat,
        resolve_dataset=ResolveDataset(),
        cache=cache if cache is not None else InMemoryCacheGateway(),
        job_queue=job_queue if job_queue is not None else InMemoryJobQueue(),
        request_rate_limiter=request_rate_limiter,
        max_queue_depth=max_queue_depth,
        default_max_limit=default_max_limit,
        inline_wait_seconds=inline_wait_seconds,
    )


def como_executada(request: QueryRequest, schema=None) -> QueryRequest:
    """A requisição como o use case a enfileira: com o teto de `limit` do schema aplicado.

    O teto entra **antes** do `query_id` (seção 2.6), então é esta — e não a que o
    cliente montou — que dá a chave de cache, o `query_id` da resposta e o payload do
    job. Os schemas do catálogo declaram `max_limit`, logo até requisição sem `limit`
    passa por aqui.
    """
    schema = schema if schema is not None else vendas_schema()
    return dataclasses.replace(request, limit=schema.effective_limit(request.limit))


def completed_for(request: QueryRequest, schema=None, **meta) -> QueryResult:
    """Resultado concluído como o worker o devolveria — para semear
    `InMemoryJobQueue.default_result` (job que conclui dentro da janela inline)."""
    schema = schema if schema is not None else vendas_schema()
    executed = como_executada(request, schema)
    return QueryResult.completed(
        query_id=executed.query_id,
        columns=schema.columns_for(executed),
        rows=(),
        dataset_used=meta.get("dataset_used", "vendas_agregado_uf"),
        execution_ms=meta.get("execution_ms", 12),
    )


# --- Caminho feliz -----------------------------------------------------------------------


async def test_caminho_feliz_da_secao_2_2_devolve_o_formato_da_secao_2_3():
    schema = vendas_schema()
    job_queue = InMemoryJobQueue()
    execute = make_execute_query(Catalog(schemas={"vendas": schema}), job_queue=job_queue)
    request = QueryRequest(
        schema="vendas",
        dimensions=("sigla_uf",),
        measures=("valor_total", "quantidade"),
        filters=(Filter(field="sigla_uf", operator=FilterOperator.IN, value=["SP", "RJ"]),),
        order_by=(),
    )
    job_queue.default_result = completed_for(request, schema)

    result = await execute(request, roles=["financeiro"], client_id="cliente-1")

    assert result.status is QueryStatus.COMPLETED
    assert result.meta.dataset_used == "vendas_agregado_uf"
    assert [(c.field, c.type.value, c.format) for c in result.columns] == [
        ("sigla_uf", "string", None),
        ("valor_total", "number", "currency"),
        ("quantidade", "number", None),
    ]
    assert job_queue.calls == [(como_executada(request, schema), "vendas_agregado_uf")]


async def test_eventos_navegacao_resolve_para_o_dataset_elasticsearch():
    job_queue = InMemoryJobQueue()
    execute = make_execute_query(
        Catalog(schemas={"eventos_navegacao": eventos_schema()}), job_queue=job_queue
    )
    request = QueryRequest(
        schema="eventos_navegacao", dimensions=("pais",), measures=("duracao_media",)
    )

    result = await execute(request, roles=[], client_id="cliente-1")

    assert result.status is QueryStatus.PROCESSING  # sem worker, a janela inline expira
    assert job_queue.calls[0][1] == "eventos_navegacao_es"


# --- Toda consulta é enfileirada e aguardada inline -------------------------------------


async def test_toda_consulta_enfileira_e_aguarda_inline():
    cache = InMemoryCacheGateway()
    job_queue = InMemoryJobQueue()
    execute = make_execute_query(
        Catalog(schemas={"vendas": vendas_schema()}), cache=cache, job_queue=job_queue
    )
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))

    result = await execute(request, roles=["financeiro"], client_id="cliente-1")

    executada = como_executada(request)
    assert result.status is QueryStatus.PROCESSING
    assert result.query_id == executada.query_id
    assert job_queue.calls == [(executada, "vendas_agregado_uf")]
    assert await cache.get(executada.cache_key) is None  # a API não grava no cache


async def test_resultado_inline_quando_o_job_conclui_no_tempo():
    schema = vendas_schema()
    job_queue = InMemoryJobQueue()
    execute = make_execute_query(Catalog(schemas={"vendas": schema}), job_queue=job_queue)
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))
    job_queue.default_result = completed_for(request, schema, execution_ms=40)

    result = await execute(request, roles=["financeiro"], client_id="cliente-1")

    assert result.status is QueryStatus.COMPLETED
    assert result.query_id == como_executada(request, schema).query_id
    assert result.meta.execution_ms == 40


async def test_timeout_inline_devolve_processing():
    job_queue = InMemoryJobQueue()
    execute = make_execute_query(
        Catalog(schemas={"vendas": vendas_schema()}), job_queue=job_queue
    )
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))

    result = await execute(request, roles=["financeiro"], client_id="cliente-1")

    assert result.status is QueryStatus.PROCESSING


async def test_job_que_falhou_no_tempo_devolve_failed():
    schema = vendas_schema()
    job_queue = InMemoryJobQueue()
    cache = InMemoryCacheGateway()
    execute = make_execute_query(
        Catalog(schemas={"vendas": schema}), cache=cache, job_queue=job_queue
    )
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))
    executada = como_executada(request, schema)
    job_queue.default_result = QueryResult.failed(executada.query_id, error="query_timeout")

    result = await execute(request, roles=["financeiro"], client_id="cliente-1")

    assert result.status is QueryStatus.FAILED
    assert await cache.get(executada.cache_key) is None


# --- Cache pelo query_id (o worker é quem grava; aqui só a leitura) ---------------------


async def test_cache_hit_curto_circuita_sem_enfileirar():
    schema = vendas_schema()
    cache = InMemoryCacheGateway()
    job_queue = InMemoryJobQueue()
    execute = make_execute_query(
        Catalog(schemas={"vendas": schema}), cache=cache, job_queue=job_queue
    )
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))
    cached = completed_for(request, schema)
    await cache.set(como_executada(request, schema).cache_key, cached)

    result = await execute(request, roles=["financeiro"], client_id="cliente-1")

    assert result.meta.cached is True
    assert result.rows == cached.rows
    assert job_queue.calls == []  # nem chegou a enfileirar


# --- Backpressure de fila cheia (seção 2.4) -------------------------------------------------


async def test_fila_cheia_recusa_qualquer_consulta_com_429():
    job_queue = InMemoryJobQueue()
    ja_na_fila = QueryRequest(schema="vendas", dimensions=("cargo",))
    await job_queue.enqueue(ja_na_fila, "vendas_detalhado")  # profundidade 1
    execute = make_execute_query(
        Catalog(schemas={"vendas": vendas_schema()}),
        job_queue=job_queue,
        max_queue_depth=1,
    )
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))

    with pytest.raises(RateLimitedError):
        await execute(request, roles=["financeiro"], client_id="cliente-1")

    assert len(job_queue.calls) == 1  # só o pré-semeado — nada novo foi enfileirado


# --- Teto de limit por schema (seção 2.6) -----------------------------------------------


async def test_teto_de_limit_por_schema_e_aplicado_e_compartilha_query_id():
    schema = dataclasses.replace(vendas_schema(), max_limit=500)
    job_queue = InMemoryJobQueue()
    execute = make_execute_query(Catalog(schemas={"vendas": schema}), job_queue=job_queue)

    over_limit = QueryRequest(schema="vendas", dimensions=("sigla_uf",), limit=10_000)
    await execute(over_limit, roles=[], client_id="cliente-1")
    at_limit = QueryRequest(schema="vendas", dimensions=("sigla_uf",), limit=500)
    await execute(at_limit, roles=[], client_id="cliente-1")

    assert job_queue.calls[0][0].limit == 500
    # Acima do teto e no teto colapsam no mesmo `query_id` (chave de cache / job).
    assert job_queue.calls[0][0].query_id == job_queue.calls[1][0].query_id


async def test_teto_padrao_protege_schema_publicado_sem_max_limit():
    """Sem `max_limit` no catálogo e sem teto de operação, a consulta sem `limit` vira
    `SELECT` sem `LIMIT`. O `default_max_limit` é o que impede isso num schema
    recém-publicado."""
    schema = dataclasses.replace(vendas_schema(), max_limit=None)
    sem_limite = QueryRequest(schema="vendas", dimensions=("sigla_uf",))

    desprotegido = InMemoryJobQueue()
    await make_execute_query(
        Catalog(schemas={"vendas": schema}), job_queue=desprotegido
    )(sem_limite, roles=[], client_id="cliente-1")
    assert desprotegido.calls[0][0].limit is None

    protegido = InMemoryJobQueue()
    await make_execute_query(
        Catalog(schemas={"vendas": schema}), job_queue=protegido, default_max_limit=50_000
    )(sem_limite, roles=[], client_id="cliente-1")
    assert protegido.calls[0][0].limit == 50_000


async def test_teto_do_schema_tem_precedencia_sobre_o_teto_padrao():
    job_queue = InMemoryJobQueue()
    execute = make_execute_query(
        Catalog(schemas={"vendas": dataclasses.replace(vendas_schema(), max_limit=500)}),
        job_queue=job_queue,
        default_max_limit=50_000,
    )

    await execute(
        QueryRequest(schema="vendas", dimensions=("sigla_uf",)),
        roles=[],
        client_id="cliente-1",
    )

    assert job_queue.calls[0][0].limit == 500


# --- Os erros da seção 2.5, todos abortando antes de enfileirar -----------------------


async def test_unknown_schema():
    job_queue = InMemoryJobQueue()
    execute = make_execute_query(catalog(), job_queue=job_queue)
    request = QueryRequest(schema="produtos", dimensions=("nome",))

    with pytest.raises(UnknownSchemaError):
        await execute(request, roles=[], client_id="cliente-1")
    assert job_queue.calls == []


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
    execute = make_execute_query(catalog())
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",))

    with pytest.raises(TypeError):
        await execute(request, roles=[])


# --- Rate limiting por cliente (Marco 9) --------------------------------------------------


async def test_sem_rate_limiter_configurado_nada_e_limitado():
    schema = vendas_schema()
    job_queue = InMemoryJobQueue()
    execute = make_execute_query(Catalog(schemas={"vendas": schema}), job_queue=job_queue)
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))
    job_queue.default_result = completed_for(request, schema)

    for _ in range(10):
        result = await execute(request, roles=["financeiro"], client_id="cliente-1")
        assert result.status is QueryStatus.COMPLETED


async def test_limite_geral_excedido_levanta_rate_limited_sem_enfileirar():
    job_queue = InMemoryJobQueue()
    limiter = InMemoryRateLimiter(limit=2)
    execute = make_execute_query(
        Catalog(schemas={"vendas": vendas_schema()}),
        job_queue=job_queue,
        request_rate_limiter=limiter,
    )
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))

    await execute(request, roles=["financeiro"], client_id="cliente-1")
    await execute(request, roles=["financeiro"], client_id="cliente-1")

    with pytest.raises(RateLimitedError):
        await execute(request, roles=["financeiro"], client_id="cliente-1")

    assert len(job_queue.calls) == 2  # a 3a nem chega a enfileirar


async def test_limite_geral_e_por_cliente_independente():
    limiter = InMemoryRateLimiter(limit=1)
    execute = make_execute_query(
        Catalog(schemas={"vendas": vendas_schema()}),
        request_rate_limiter=limiter,
    )
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))

    await execute(request, roles=["financeiro"], client_id="cliente-a")
    await execute(request, roles=["financeiro"], client_id="cliente-b")

    with pytest.raises(RateLimitedError):
        await execute(request, roles=["financeiro"], client_id="cliente-a")
