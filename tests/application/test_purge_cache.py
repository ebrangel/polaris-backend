"""`PurgeCache` — limpeza forçada do cache, tudo ou por schema. Só orquestra
`CacheGateway.clear`; a semântica de prefixo é do adapter (aqui, o fake in-memory)."""

from fixtures import vendas_schema

from application.use_cases import PurgeCache
from domain.models import QueryRequest, QueryResult
from fakes import InMemoryCacheGateway


def _result(request: QueryRequest) -> QueryResult:
    schema = vendas_schema()
    return QueryResult.completed(
        query_id=request.query_id,
        columns=schema.columns_for(request),
        rows=(("SP", 100.0),),
        dataset_used="vendas_agregado_uf",
    )


async def _seed(cache: InMemoryCacheGateway) -> None:
    # `limit` distinto só para variar o hash e gerar uma chave por requisição.
    for schema_name, limit in (("vendas", 10), ("vendas", 20), ("rh", 10)):
        request = QueryRequest(
            schema=schema_name,
            dimensions=("sigla_uf",),
            measures=("valor_total",),
            limit=limit,
        )
        await cache.set(request.cache_key, _result(request))


async def test_purga_apenas_o_schema_informado():
    cache = InMemoryCacheGateway()
    await _seed(cache)
    purge = PurgeCache(cache)

    purged = await purge(schema="vendas")

    assert purged == 2
    assert all(not key.startswith("vendas:") for key in cache._store)
    assert any(key.startswith("rh:") for key in cache._store)


async def test_sem_schema_purga_tudo():
    cache = InMemoryCacheGateway()
    await _seed(cache)
    purge = PurgeCache(cache)

    purged = await purge()

    assert purged == 3
    assert cache._store == {}


async def test_schema_inexistente_e_no_op():
    cache = InMemoryCacheGateway()
    await _seed(cache)
    purge = PurgeCache(cache)

    assert await purge(schema="nao_existe") == 0
    assert len(cache._store) == 3
