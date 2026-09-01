"""Ports do Marco 2 — conformidade estrutural dos fakes + o comportamento prometido
nos docstrings. Sem implementação concreta ainda: só os contratos e os fakes
in-memory que o Marco 4 vai reutilizar para testar a orquestração do `ExecuteQuery`.
"""

import inspect

import pytest
from elasticsearch import AsyncElasticsearch
from fixtures import vendas_agregado_uf, vendas_schema
from sqlalchemy.ext.asyncio import create_async_engine

from adapters.executors import ElasticsearchQueryExecutor, SQLAlchemyQueryExecutor
from application.catalog_codec import canonical_json, schema_to_dict
from application.ports.cache_gateway import CacheGateway
from application.ports.catalog_invalidator import CatalogInvalidator
from application.ports.catalog_repository import CatalogRepository
from application.ports.datasource_inspector import DatasourceInspector
from application.ports.job_queue import JobQueue
from application.ports.query_executor import QueryExecutor
from application.ports.rate_limiter import RateLimiter
from application.ports.result_exporter import ResultExporter
from domain.errors import QueryTimeoutError
from domain.models import QueryRequest, QueryResult
from fakes import (
    InMemoryCacheGateway,
    InMemoryCatalogInvalidator,
    InMemoryCatalogRepository,
    InMemoryJobQueue,
    InMemoryRateLimiter,
    InMemoryResultExporter,
    StubDatasourceInspector,
    StubQueryExecutor,
)

#: (Protocol, classe do fake, métodos declarados no Protocol) — explícito de propósito,
#: para que a lista fique visível na leitura do teste, sem depender de atributos
#: internos do `typing.Protocol`.
PORT_METHODS = {
    CatalogRepository: (
        InMemoryCatalogRepository,
        ("get_active_version", "list_active_versions", "publish_new_version"),
    ),
    QueryExecutor: (StubQueryExecutor, ("execute",)),
    CacheGateway: (InMemoryCacheGateway, ("get", "set", "delete", "stats")),
    JobQueue: (InMemoryJobQueue, ("enqueue", "wait_for_result", "get_status", "depth")),
    DatasourceInspector: (StubDatasourceInspector, ("missing_fields",)),
    CatalogInvalidator: (InMemoryCatalogInvalidator, ("publish",)),
    RateLimiter: (InMemoryRateLimiter, ("allow",)),
    ResultExporter: (
        InMemoryResultExporter,
        ("export", "stat", "open", "purge_expired"),
    ),
}


# --- Conformidade estrutural -----------------------------------------------------------


@pytest.mark.parametrize(
    "port, pair",
    PORT_METHODS.items(),
    ids=[p.__name__ for p in PORT_METHODS],
)
def test_fake_satisfaz_o_protocol(port, pair):
    """`@runtime_checkable` só confere a existência dos métodos — é o que basta para
    `isinstance`, mas não travaria um fake com um método faltando por engano."""
    fake_class, _ = pair
    fake = fake_class()

    assert isinstance(fake, port)


def _assert_signature_matches(port: type, method_name: str, impl: object) -> None:
    """`isinstance` não compara parâmetros nem sincronia — comparamos à mão para que um
    adapter (fake ou real) que mude a ordem dos parâmetros ou vire síncrono quebre aqui,
    e não silenciosamente ao rodar contra um banco de verdade."""
    port_method = getattr(port, method_name)
    impl_method = getattr(impl, method_name)

    assert inspect.iscoroutinefunction(port_method), (
        f"{port.__name__}.{method_name} precisa ser `async def`"
    )
    assert inspect.iscoroutinefunction(impl_method), (
        f"{type(impl).__name__}.{method_name} precisa ser `async def`"
    )

    port_params = list(inspect.signature(port_method).parameters)[1:]  # sem `self`
    impl_params = list(inspect.signature(impl_method).parameters)
    assert port_params == impl_params, (
        f"{method_name}: port pede {port_params}, {type(impl).__name__} declara {impl_params}"
    )


@pytest.mark.parametrize(
    "port, pair",
    PORT_METHODS.items(),
    ids=[p.__name__ for p in PORT_METHODS],
)
def test_metodos_do_fake_tem_a_mesma_assinatura_do_port(port, pair):
    fake_class, method_names = pair
    fake = fake_class()

    for method_name in method_names:
        _assert_signature_matches(port, method_name, fake)


def test_todos_os_ports_sao_runtime_checkable():
    for port in PORT_METHODS:
        assert getattr(port, "_is_runtime_protocol", False), (
            f"{port.__name__} precisa de @runtime_checkable"
        )


# --- Conformidade dos executores reais do Marco 5 ---------------------------------------
#
# `engine`/`client` "preguiçosos": não abrem conexão nenhuma até o primeiro uso, então
# este teste roda sem Docker e sem rede — só confere forma, não comportamento (isso é
# `tests/adapters/executors/test_postgres_integration.py` e `test_elasticsearch_integration.py`).


def _real_query_executors():
    engine = create_async_engine("postgresql+psycopg://user:pass@localhost/db")
    client = AsyncElasticsearch(hosts=["http://localhost:9200"])
    return [
        SQLAlchemyQueryExecutor(engine=engine),
        ElasticsearchQueryExecutor(client=client),
    ]


@pytest.mark.parametrize(
    "impl", _real_query_executors(), ids=lambda impl: type(impl).__name__
)
def test_executor_real_satisfaz_o_protocol(impl):
    assert isinstance(impl, QueryExecutor)


@pytest.mark.parametrize(
    "impl", _real_query_executors(), ids=lambda impl: type(impl).__name__
)
def test_executor_real_tem_a_mesma_assinatura_do_port(impl):
    for method_name in ("execute",):
        _assert_signature_matches(QueryExecutor, method_name, impl)


# --- CacheGateway ------------------------------------------------------------------------


@pytest.fixture
def sample_result() -> QueryResult:
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))
    columns = vendas_schema().columns_for(request)
    return QueryResult.completed(
        query_id=request.query_id,
        columns=columns,
        rows=(("SP", 100.0),),
        dataset_used="vendas_agregado_uf",
    )


async def test_cache_gateway_get_em_chave_ausente_devolve_none():
    cache = InMemoryCacheGateway()

    assert await cache.get("q_inexistente") is None
    assert cache.misses == 1
    assert cache.hits == 0


async def test_cache_gateway_set_depois_get_devolve_o_mesmo_resultado(sample_result):
    """A chave usada é `QueryRequest.query_id` (seção 3), já implementado no Marco 1."""
    cache = InMemoryCacheGateway()

    await cache.set(sample_result.query_id, sample_result)
    cached = await cache.get(sample_result.query_id)

    assert cached == sample_result
    assert cache.hits == 1


async def test_cache_gateway_recusa_resultado_nao_concluido():
    cache = InMemoryCacheGateway()
    processing = QueryResult.processing("q_abc123")

    with pytest.raises(ValueError, match="completed"):
        await cache.set("q_abc123", processing)


async def test_cache_gateway_delete_remove_a_entrada(sample_result):
    cache = InMemoryCacheGateway()
    await cache.set(sample_result.query_id, sample_result)

    await cache.delete(sample_result.query_id)

    assert await cache.get(sample_result.query_id) is None


async def test_cache_gateway_stats_acompanha_hits_e_misses(sample_result):
    cache = InMemoryCacheGateway()
    await cache.set(sample_result.query_id, sample_result)

    await cache.get(sample_result.query_id)  # hit
    await cache.get("q_inexistente")  # miss
    await cache.get("q_inexistente")  # miss

    stats = await cache.stats()
    assert stats.hits == 1
    assert stats.misses == 2


# --- JobQueue ------------------------------------------------------------------------------


async def test_job_queue_enqueue_devolve_o_corpo_do_202_da_secao_2_4():
    queue = InMemoryJobQueue()
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf", "cargo"))

    result = await queue.enqueue(request, dataset_name="vendas_detalhado")

    assert result.query_id == request.query_id
    assert result.status.value == "processing"


async def test_job_queue_get_status_de_query_id_desconhecido_e_none():
    queue = InMemoryJobQueue()

    assert await queue.get_status("q_000000") is None


async def test_job_queue_wait_for_result_devolve_processing_enquanto_o_job_roda():
    queue = InMemoryJobQueue()
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",))
    await queue.enqueue(request, dataset_name="vendas_agregado_uf")

    result = await queue.wait_for_result(request.query_id, timeout=0.01)

    assert result.status.value == "processing"
    assert result.query_id == request.query_id


async def test_job_queue_wait_for_result_devolve_o_resultado_final(sample_result):
    queue = InMemoryJobQueue()
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))
    await queue.enqueue(request, dataset_name="vendas_agregado_uf")
    queue.resolve(request.query_id, sample_result)

    assert await queue.wait_for_result(request.query_id, timeout=0.01) == sample_result


async def test_job_queue_wait_for_result_de_query_id_desconhecido_e_processing():
    queue = InMemoryJobQueue()

    result = await queue.wait_for_result("q_000000", timeout=0.01)

    assert result.status.value == "processing"


async def test_job_queue_depth_acompanha_jobs_pendentes(sample_result):
    queue = InMemoryJobQueue()
    a = QueryRequest(schema="vendas", dimensions=("sigla_uf",))
    b = QueryRequest(schema="vendas", measures=("quantidade",))

    await queue.enqueue(a, dataset_name="vendas_agregado_uf")
    await queue.enqueue(b, dataset_name="vendas_agregado_uf")
    assert await queue.depth() == 2

    queue.resolve(a.query_id, sample_result)
    assert await queue.depth() == 1
    assert (await queue.get_status(a.query_id)) == sample_result


# --- CatalogRepository ---------------------------------------------------------------------


async def test_catalog_repository_get_active_version_de_schema_desconhecido():
    repo = InMemoryCatalogRepository()

    assert await repo.get_active_version("vendas") is None


async def test_catalog_repository_publish_new_version():
    repo = InMemoryCatalogRepository()
    schema = vendas_schema()
    content = canonical_json(schema_to_dict(schema))

    version = await repo.publish_new_version(
        schema_name="vendas",
        content=content,
        content_hash="abc123",
        git_sha="deadbeef",
        published_by="pipeline",
    )

    assert version.schema == schema
    assert version.is_active
    assert version.published_by == "pipeline"
    assert (await repo.get_active_version("vendas")) == version
    assert version in (await repo.list_active_versions())


async def test_catalog_repository_nova_publicacao_desativa_a_anterior():
    """"Cada publicação insere uma nova linha (nunca UPDATE); a anterior é desativada
    na mesma transação" — `docs/pipeline-publicacao.md`."""
    repo = InMemoryCatalogRepository()
    content = canonical_json(schema_to_dict(vendas_schema()))

    first = await repo.publish_new_version("vendas", content, "hash1", "sha1")
    second = await repo.publish_new_version("vendas", content, "hash2", "sha2")

    assert (await repo.get_active_version("vendas")) == second
    assert repo.history("vendas") == (first, second)
    active = await repo.list_active_versions()
    assert first not in active
    assert second in active


# --- QueryExecutor -------------------------------------------------------------------------


async def test_query_executor_execute_devolve_o_resultado_programado(sample_result):
    executor = StubQueryExecutor(result=sample_result)
    dataset = vendas_agregado_uf()
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))
    columns = vendas_schema().columns_for(request)

    result = await executor.execute(dataset, request, columns)

    assert result == sample_result
    assert executor.calls == [(dataset, request, columns)]


async def test_query_executor_execute_sem_resultado_programado_monta_um_completed():
    executor = StubQueryExecutor()
    dataset = vendas_agregado_uf()
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",))
    columns = vendas_schema().columns_for(request)

    result = await executor.execute(dataset, request, columns)

    assert result.status.value == "completed"
    assert result.query_id == request.query_id
    assert result.meta.dataset_used == dataset.name


async def test_query_executor_propaga_erro_programado():
    executor = StubQueryExecutor(raises=QueryTimeoutError("estourou o timeout"))
    dataset = vendas_agregado_uf()
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",))

    with pytest.raises(QueryTimeoutError):
        await executor.execute(dataset, request, ())


# --- RateLimiter (Marco 9) -----------------------------------------------------------------


async def test_rate_limiter_permite_ate_o_limite_e_recusa_depois():
    limiter = InMemoryRateLimiter(limit=2)

    assert await limiter.allow("cliente-1") is True
    assert await limiter.allow("cliente-1") is True
    assert await limiter.allow("cliente-1") is False


async def test_rate_limiter_contadores_independentes_por_cliente():
    limiter = InMemoryRateLimiter(limit=1)

    assert await limiter.allow("cliente-a") is True
    assert await limiter.allow("cliente-b") is True  # outro cliente, outro contador
    assert await limiter.allow("cliente-a") is False
