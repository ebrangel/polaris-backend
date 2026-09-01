"""`POST /internal/cache/purge` — mesmo padrão de proteção do `admin.py` /
`observability.py`, com os fakes do Marco 2: sem banco, sem Redis."""

import pytest
from fastapi.testclient import TestClient

from adapters.api import create_app
from application.use_cases import ExecuteQuery, PurgeCache, ResolveDataset
from domain.models import Catalog, QueryRequest, QueryResult
from fakes import InMemoryCacheGateway, InMemoryJobQueue

_INTERNAL_TOKEN = "token-interno-de-teste"


def _completed(request: QueryRequest) -> QueryResult:
    return QueryResult.completed(
        query_id=request.query_id, columns=(), rows=(), dataset_used="d"
    )


@pytest.fixture
def cache() -> InMemoryCacheGateway:
    return InMemoryCacheGateway()


@pytest.fixture
def cache_client(cache) -> TestClient:
    """App com `/internal/cache` montado — só acontece quando `purge_cache` é passado."""
    catalog = Catalog(schemas={})
    job_queue = InMemoryJobQueue()
    app = create_app(
        catalog=catalog,
        execute_query=ExecuteQuery(
            catalog=catalog,
            resolve_dataset=ResolveDataset(),
            cache=cache,
            job_queue=job_queue,
        ),
        job_queue=job_queue,
        purge_cache=PurgeCache(cache),
        internal_token=_INTERNAL_TOKEN,
    )
    return TestClient(app)


async def _seed(cache: InMemoryCacheGateway) -> None:
    for schema_name, limit in (("vendas", 10), ("vendas", 20), ("rh", 10)):
        request = QueryRequest(
            schema=schema_name, dimensions=("sigla_uf",), measures=("valor_total",), limit=limit
        )
        await cache.set(request.cache_key, _completed(request))


# --- Autenticação ----------------------------------------------------------------------


def test_sem_header_e_401(cache_client):
    assert cache_client.post("/internal/cache/purge").status_code == 401


def test_com_token_errado_e_401(cache_client):
    response = cache_client.post(
        "/internal/cache/purge", headers={"X-Internal-Token": "errado"}
    )
    assert response.status_code == 401


# --- Purga ---------------------------------------------------------------------------------


async def test_purga_por_schema(cache_client, cache):
    await _seed(cache)

    response = cache_client.post(
        "/internal/cache/purge?schema=vendas",
        headers={"X-Internal-Token": _INTERNAL_TOKEN},
    )

    assert response.status_code == 200
    assert response.json() == {"purged": 2, "schema": "vendas"}
    assert [key for key in cache._store if key.startswith("vendas:")] == []
    assert [key for key in cache._store if key.startswith("rh:")] != []


async def test_purga_tudo_quando_sem_schema(cache_client, cache):
    await _seed(cache)

    response = cache_client.post(
        "/internal/cache/purge", headers={"X-Internal-Token": _INTERNAL_TOKEN}
    )

    assert response.status_code == 200
    assert response.json() == {"purged": 3, "schema": None}
    assert cache._store == {}


async def test_schema_inexistente_e_no_op(cache_client, cache):
    await _seed(cache)

    response = cache_client.post(
        "/internal/cache/purge?schema=fantasma",
        headers={"X-Internal-Token": _INTERNAL_TOKEN},
    )

    assert response.status_code == 200
    assert response.json() == {"purged": 0, "schema": "fantasma"}
    assert len(cache._store) == 3


# --- Sem purge_cache: router nem existe ---------------------------------------------------


def test_sem_purge_cache_o_router_nao_e_montado(client):
    """O `client` do `conftest.py` (Marco 6) chama `create_app` sem `purge_cache` — a
    rota precisa dar 404, não 401: ela simplesmente não existe."""
    assert client.post("/internal/cache/purge").status_code == 404
