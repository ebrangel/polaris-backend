"""Rate limiting por cliente (Marco 9), ponta a ponta via `TestClient` — identidade do
cliente pelo header `X-Api-Key` (stand-in, mesmo espírito de `X-Roles`), com fallback
por IP quando ausente. Usa `InMemoryRateLimiter`, não Redis — o comportamento do
contador de janela fixa em si é coberto por `test_redis_rate_limiter.py` (Docker).
"""

from fastapi.testclient import TestClient
from fixtures import catalog as catalog_fixture
from httpx import Response

from adapters.api import create_app
from application.use_cases import ExecuteQuery, ResolveDataset
from fakes import InMemoryCacheGateway, InMemoryJobQueue, InMemoryRateLimiter, StubQueryExecutor

_ALL_CONNECTION_REFS = (
    "env:DW_VENDAS_PG_URL",
    "env:DW_VENDAS_ORACLE_URL",
    "env:ES_EVENTOS_URL",
    "env:APP_ESTOQUE_URL",
)


def _client_with_request_limit(limit: int) -> tuple[TestClient, InMemoryRateLimiter]:
    catalog = catalog_fixture()
    limiter = InMemoryRateLimiter(limit=limit)
    execute_query = ExecuteQuery(
        catalog=catalog,
        resolve_dataset=ResolveDataset(),
        executors=dict.fromkeys(_ALL_CONNECTION_REFS, StubQueryExecutor()),
        cache=InMemoryCacheGateway(),
        job_queue=InMemoryJobQueue(),
        request_rate_limiter=limiter,
    )
    app = create_app(catalog=catalog, execute_query=execute_query, job_queue=InMemoryJobQueue())
    return TestClient(app), limiter


def _query(client: TestClient, *, api_key: str | None = None) -> Response:
    headers = {"X-Roles": "financeiro"}
    if api_key is not None:
        headers["X-Api-Key"] = api_key
    return client.post(
        "/v1/query",
        json={"schema": "vendas", "dimensions": ["sigla_uf"], "measures": ["valor_total"]},
        headers=headers,
    )


def test_cliente_estoura_o_limite_geral_recebe_429():
    client, _ = _client_with_request_limit(limit=2)

    assert _query(client, api_key="cliente-1").status_code == 200
    assert _query(client, api_key="cliente-1").status_code == 200

    response = _query(client, api_key="cliente-1")

    assert response.status_code == 429
    assert response.json()["type"] == "rate_limited"


def test_dois_clientes_tem_contadores_independentes():
    client, _ = _client_with_request_limit(limit=1)

    assert _query(client, api_key="cliente-a").status_code == 200
    # `cliente-b` não herda o consumo de `cliente-a` — outro `X-Api-Key`, outro balde.
    assert _query(client, api_key="cliente-b").status_code == 200

    assert _query(client, api_key="cliente-a").status_code == 429
    assert _query(client, api_key="cliente-b").status_code == 429


def test_sem_x_api_key_cai_no_fallback_por_ip():
    """Sem o header, o cliente ainda é identificado (pelo IP do socket) — omitir
    `X-Api-Key` não é uma forma de escapar do rate limiting."""
    client, limiter = _client_with_request_limit(limit=1)

    assert _query(client).status_code == 200
    response = _query(client)

    assert response.status_code == 429
    assert all(call.startswith("ip:") for call in limiter.calls)
