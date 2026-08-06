"""`GET /internal/observability` — mesmo padrão de proteção do `admin.py` (Marco 8),
com os fakes do Marco 2/9: sem banco, sem Redis.
"""

import pytest
from fastapi.testclient import TestClient

from adapters.api import create_app
from application.use_cases import ExecuteQuery, GetObservabilitySnapshot, ResolveDataset
from domain.models import Catalog, Column, DataType, QueryRequest, QueryResult
from fakes import InMemoryCacheGateway, InMemoryJobQueue

_INTERNAL_TOKEN = "token-interno-de-teste"


@pytest.fixture
def cache() -> InMemoryCacheGateway:
    return InMemoryCacheGateway()


@pytest.fixture
def job_queue() -> InMemoryJobQueue:
    return InMemoryJobQueue()


@pytest.fixture
def get_observability_snapshot(cache, job_queue) -> GetObservabilitySnapshot:
    return GetObservabilitySnapshot(cache=cache, job_queue=job_queue)


@pytest.fixture
def observability_client(cache, job_queue, get_observability_snapshot) -> TestClient:
    """App com `/internal/observability` montado — só acontece quando
    `get_observability_snapshot` é passado a `create_app`."""
    catalog = Catalog(schemas={})
    execute_query = ExecuteQuery(
        catalog=catalog,
        resolve_dataset=ResolveDataset(),
        executors={},
        cache=cache,
        job_queue=job_queue,
    )
    app = create_app(
        catalog=catalog,
        execute_query=execute_query,
        job_queue=job_queue,
        get_observability_snapshot=get_observability_snapshot,
        internal_token=_INTERNAL_TOKEN,
    )
    return TestClient(app)


# --- Autenticação -------------------------------------------------------------------------


def test_sem_header_e_401(observability_client):
    response = observability_client.get("/internal/observability")

    assert response.status_code == 401


def test_com_token_errado_e_401(observability_client):
    response = observability_client.get(
        "/internal/observability", headers={"X-Internal-Token": "token-errado"}
    )

    assert response.status_code == 401


# --- Payload --------------------------------------------------------------------------------


def test_com_token_correto_devolve_o_snapshot(observability_client):
    response = observability_client.get(
        "/internal/observability", headers={"X-Internal-Token": _INTERNAL_TOKEN}
    )

    assert response.status_code == 200
    body = response.json()
    assert body == {
        "cache": {"hits": 0, "misses": 0, "hit_rate": 0.0},
        "heavy_queue": {"depth": 0},
    }


async def test_reflete_hits_misses_e_profundidade_reais(
    observability_client, cache, job_queue
):
    await cache.get("q_ausente")  # miss
    result = QueryResult.completed(
        query_id="q_presente",
        columns=(Column(field="sigla_uf", type=DataType.STRING),),
        rows=(("SP",),),
        dataset_used="vendas_agregado_uf",
    )
    await cache.set("q_presente", result)
    await cache.get("q_presente")  # hit
    await job_queue.enqueue(
        QueryRequest(schema="vendas", dimensions=("sigla_uf",)), "vendas_agregado_uf"
    )

    response = observability_client.get(
        "/internal/observability", headers={"X-Internal-Token": _INTERNAL_TOKEN}
    )

    body = response.json()
    assert body["cache"] == {"hits": 1, "misses": 1, "hit_rate": 0.5}
    assert body["heavy_queue"] == {"depth": 1}


# --- Sem get_observability_snapshot: router nem existe --------------------------------------


def test_sem_get_observability_snapshot_o_router_nao_e_montado(client):
    """O `client` do `conftest.py` (Marco 6) chama `create_app` sem
    `get_observability_snapshot` — a rota precisa dar 404, não 401: ela simplesmente
    não existe."""
    response = client.get("/internal/observability")

    assert response.status_code == 404
