"""`ExecuteQuery` — o fluxo completo da seção 3: validar, autorizar, aplicar o teto de
`limit`, checar o cache, resolver o dataset, estimar custo (enfileirar se pesada —
seção 2.4), executar, cachear.
"""

import dataclasses

import pytest
from fixtures import catalog, eventos_schema, vendas_schema, vendas_schema_com_canal

from application.ports.query_executor import QueryCost
from application.use_cases import ExecuteQuery, ResolveDataset
from domain.errors import (
    ForbiddenMeasureError,
    InvalidFilterError,
    NoDatasetAvailableError,
    QueryTimeoutError,
    UnknownFieldError,
    UnknownSchemaError,
)
from domain.models import (
    Catalog,
    DatasourceType,
    Filter,
    FilterOperator,
    QueryRequest,
    QueryResult,
    QueryStatus,
)
from fakes import InMemoryCacheGateway, InMemoryJobQueue, StubQueryExecutor


def make_execute_query(cat, *, executors=None, cache=None, job_queue=None):
    return ExecuteQuery(
        catalog=cat,
        resolve_dataset=ResolveDataset(),
        executors=executors if executors is not None else {},
        cache=cache if cache is not None else InMemoryCacheGateway(),
        job_queue=job_queue if job_queue is not None else InMemoryJobQueue(),
    )


# --- Caminho feliz -----------------------------------------------------------------------


async def test_caminho_feliz_da_secao_2_2_devolve_o_formato_da_secao_2_3():
    schema = vendas_schema()
    stub = StubQueryExecutor()
    execute = make_execute_query(
        Catalog(schemas={"vendas": schema}), executors={DatasourceType.POSTGRES: stub}
    )
    request = QueryRequest(
        schema="vendas",
        dimensions=("sigla_uf",),
        measures=("valor_total", "quantidade"),
        filters=(Filter(field="sigla_uf", operator=FilterOperator.IN, value=["SP", "RJ"]),),
        order_by=(),
    )

    result = await execute(request, roles=["financeiro"])

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
        executors={DatasourceType.ELASTICSEARCH: stub},
    )
    request = QueryRequest(
        schema="eventos_navegacao", dimensions=("pais",), measures=("duracao_media",)
    )

    result = await execute(request, roles=[])

    assert result.status is QueryStatus.COMPLETED
    assert len(stub.calls) == 1


# --- Cache pelo query_id -------------------------------------------------------------------


async def test_cache_miss_grava_e_cache_hit_nao_executa_de_novo():
    stub = StubQueryExecutor()
    cache = InMemoryCacheGateway()
    execute = make_execute_query(
        Catalog(schemas={"vendas": vendas_schema()}),
        executors={DatasourceType.POSTGRES: stub},
        cache=cache,
    )
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))

    first = await execute(request, roles=["financeiro"])
    assert len(stub.calls) == 1
    assert first.meta.cached is False
    assert await cache.get(request.query_id) == first

    second = await execute(request, roles=["financeiro"])

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
        executors={DatasourceType.POSTGRES: stub},
        cache=cache,
    )

    result = await execute(request, roles=[])

    assert result.status is QueryStatus.FAILED
    assert await cache.get(request.query_id) is None


async def test_erro_do_executor_propaga_e_nao_e_cacheado():
    schema = vendas_schema()
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",))
    stub = StubQueryExecutor(raises=QueryTimeoutError("estourou o timeout"))
    cache = InMemoryCacheGateway()
    execute = make_execute_query(
        Catalog(schemas={"vendas": schema}),
        executors={DatasourceType.POSTGRES: stub},
        cache=cache,
    )

    with pytest.raises(QueryTimeoutError):
        await execute(request, roles=[])

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
            DatasourceType.POSTGRES: postgres_stub,
            DatasourceType.ORACLE: oracle_stub,
        },
    )

    only_uf = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))
    await execute(only_uf, roles=["financeiro"])
    assert len(postgres_stub.calls) == 1
    assert len(oracle_stub.calls) == 0

    with_cargo = QueryRequest(
        schema="vendas", dimensions=("sigla_uf", "cargo"), measures=("valor_total",)
    )
    await execute(with_cargo, roles=["financeiro"])
    assert len(oracle_stub.calls) == 1


async def test_executor_nao_configurado_levanta_lookup_error():
    execute = make_execute_query(Catalog(schemas={"vendas": vendas_schema()}), executors={})
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",))

    with pytest.raises(LookupError, match="vendas_agregado_uf"):
        await execute(request, roles=[])


# --- Custo estimado e fila (seção 2.4) --------------------------------------------------


async def test_consulta_pesada_enfileira_sem_executar_nem_cachear():
    heavy_cost = QueryCost(score=100, threshold=50)
    stub = StubQueryExecutor(cost=heavy_cost)
    cache = InMemoryCacheGateway()
    job_queue = InMemoryJobQueue()
    execute = make_execute_query(
        Catalog(schemas={"vendas": vendas_schema()}),
        executors={DatasourceType.POSTGRES: stub},
        cache=cache,
        job_queue=job_queue,
    )
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))

    result = await execute(request, roles=["financeiro"])

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
        executors={DatasourceType.POSTGRES: StubQueryExecutor(cost=light_cost)},
        job_queue=job_queue,
    )
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))

    result = await execute(request, roles=["financeiro"])

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
        executors={DatasourceType.POSTGRES: stub},
        cache=cache,
        job_queue=job_queue,
    )
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))

    first = await execute(request, roles=["financeiro"])
    assert first.status is QueryStatus.COMPLETED

    stub.cost = QueryCost(score=100, threshold=50)  # a partir daqui, "pesada"
    second = await execute(request, roles=["financeiro"])

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
        executors={DatasourceType.POSTGRES: stub},
        cache=cache,
    )

    over_limit = QueryRequest(schema="vendas", dimensions=("sigla_uf",), limit=10_000)
    await execute(over_limit, roles=[])

    executed_request = stub.calls[0][1]
    assert executed_request.limit == 500

    at_limit = QueryRequest(schema="vendas", dimensions=("sigla_uf",), limit=500)
    result = await execute(at_limit, roles=[])

    assert len(stub.calls) == 1  # mesma entrada de cache, não executou de novo
    assert result.meta.cached is True


# --- Os erros da seção 2.5, todos abortando antes do executor --------------------------


async def test_unknown_schema():
    execute = make_execute_query(catalog())
    request = QueryRequest(schema="produtos", dimensions=("nome",))

    with pytest.raises(UnknownSchemaError):
        await execute(request, roles=[])


async def test_unknown_field():
    execute = make_execute_query(catalog())
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf", "canal"))

    with pytest.raises(UnknownFieldError):
        await execute(request, roles=[])


async def test_invalid_filter():
    """`between` não é válido para dimensões `string` (seção 2.2)."""
    execute = make_execute_query(catalog())
    request = QueryRequest(
        schema="vendas",
        dimensions=("sigla_uf",),
        filters=(Filter(field="sigla_uf", operator=FilterOperator.BETWEEN, value=["A", "Z"]),),
    )

    with pytest.raises(InvalidFilterError):
        await execute(request, roles=[])


async def test_forbidden_measure():
    execute = make_execute_query(catalog())
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))

    with pytest.raises(ForbiddenMeasureError):
        await execute(request, roles=["comercial"])


async def test_no_dataset_available():
    schema = vendas_schema_com_canal()
    execute = make_execute_query(Catalog(schemas={"vendas": schema}))
    request = QueryRequest(
        schema="vendas",
        dimensions=("sigla_uf", "cargo", "canal"),
        measures=("valor_total",),
    )

    with pytest.raises(NoDatasetAvailableError):
        await execute(request, roles=["financeiro"])


# --- roles é obrigatório -----------------------------------------------------------------


async def test_roles_e_keyword_only_obrigatorio():
    execute = make_execute_query(catalog())
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",))

    with pytest.raises(TypeError):
        await execute(request)
