"""`/internal/catalog/publish` e `/internal/catalog/reload` — o lado HTTP do pipeline de
publicação (Marco 8), com os fakes do Marco 2/8: sem banco, sem Redis.
"""

import pytest
from fastapi.testclient import TestClient

from adapters.api import create_app
from adapters.catalog.yaml_loader import DEFAULT_SCHEMAS_DIR, load_schema_file
from application.use_cases import ExecuteQuery, PublishCatalog, ResolveDataset
from domain.models import Catalog
from fakes import (
    InMemoryCacheGateway,
    InMemoryCatalogInvalidator,
    InMemoryCatalogRepository,
    InMemoryJobQueue,
)

_INTERNAL_TOKEN = "token-interno-de-teste"


@pytest.fixture
def repository() -> InMemoryCatalogRepository:
    return InMemoryCatalogRepository()


@pytest.fixture
def invalidator() -> InMemoryCatalogInvalidator:
    return InMemoryCatalogInvalidator()


@pytest.fixture
def publish_catalog(repository, invalidator) -> PublishCatalog:
    return PublishCatalog(repository=repository, inspectors={}, invalidator=invalidator)


@pytest.fixture
def admin_client(repository, publish_catalog) -> TestClient:
    """App com o router `/internal/catalog/*` montado — só acontece quando
    `publish_catalog`/`catalog_repository` são passados para `create_app`."""
    catalog = Catalog(schemas={})
    execute_query = ExecuteQuery(
        catalog=catalog,
        resolve_dataset=ResolveDataset(),
        executors={},
        cache=InMemoryCacheGateway(),
        job_queue=InMemoryJobQueue(),
    )
    app = create_app(
        catalog=catalog,
        execute_query=execute_query,
        job_queue=InMemoryJobQueue(),
        publish_catalog=publish_catalog,
        catalog_repository=repository,
        internal_token=_INTERNAL_TOKEN,
    )
    return TestClient(app)


def _estoque_publish_body(**overrides) -> dict:
    body = {"data": load_schema_file(DEFAULT_SCHEMAS_DIR / "estoque.yaml"), "git_sha": "abc123"}
    body.update(overrides)
    return body


# --- Autenticação -------------------------------------------------------------------------


def test_publish_sem_header_e_401(admin_client):
    response = admin_client.post("/internal/catalog/publish", json=_estoque_publish_body())

    assert response.status_code == 401


def test_publish_com_token_errado_e_401(admin_client):
    response = admin_client.post(
        "/internal/catalog/publish",
        json=_estoque_publish_body(),
        headers={"X-Internal-Token": "token-errado"},
    )

    assert response.status_code == 401


def test_reload_sem_header_e_401(admin_client):
    response = admin_client.post("/internal/catalog/reload")

    assert response.status_code == 401


# --- Publish --------------------------------------------------------------------------------


def test_publish_com_token_correto_publica_o_schema(admin_client, repository):
    response = admin_client.post(
        "/internal/catalog/publish",
        json=_estoque_publish_body(),
        headers={"X-Internal-Token": _INTERNAL_TOKEN},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["published"] is True
    assert body["schema_name"] == "estoque"


async def test_publish_grava_a_versao_no_repositorio(admin_client, repository):
    admin_client.post(
        "/internal/catalog/publish",
        json=_estoque_publish_body(),
        headers={"X-Internal-Token": _INTERNAL_TOKEN},
    )

    version = await repository.get_active_version("estoque")
    assert version is not None
    assert version.git_sha == "abc123"


def test_publish_republicando_o_mesmo_conteudo_nao_publica_de_novo(admin_client):
    headers = {"X-Internal-Token": _INTERNAL_TOKEN}
    body = _estoque_publish_body()

    admin_client.post("/internal/catalog/publish", json=body, headers=headers)
    response = admin_client.post("/internal/catalog/publish", json=body, headers=headers)

    assert response.json()["published"] is False


def test_publish_reporta_datasets_sem_inspecao(admin_client):
    response = admin_client.post(
        "/internal/catalog/publish",
        json=_estoque_publish_body(),
        headers={"X-Internal-Token": _INTERNAL_TOKEN},
    )

    assert response.json()["uninspected_datasets"] == ["estoque_atual_pg"]


def test_publish_campo_extra_no_corpo_e_recusado(admin_client):
    response = admin_client.post(
        "/internal/catalog/publish",
        json=_estoque_publish_body(campo_desconhecido="x"),
        headers={"X-Internal-Token": _INTERNAL_TOKEN},
    )

    assert response.status_code == 422


# --- Reload ---------------------------------------------------------------------------------


def test_reload_recarrega_o_catalogo_em_memoria(admin_client):
    headers = {"X-Internal-Token": _INTERNAL_TOKEN}
    admin_client.post("/internal/catalog/publish", json=_estoque_publish_body(), headers=headers)

    response = admin_client.post("/internal/catalog/reload", headers=headers)

    assert response.status_code == 200
    body = response.json()
    assert body["reloaded"] is True
    assert body["schemas"] == ["estoque"]


def test_reload_sem_nenhuma_publicacao_devolve_catalogo_vazio(admin_client):
    response = admin_client.post(
        "/internal/catalog/reload", headers={"X-Internal-Token": _INTERNAL_TOKEN}
    )

    assert response.json()["schemas"] == []


# --- Sem publish_catalog/catalog_repository: router nem existe -----------------------------


def test_sem_publish_catalog_o_router_admin_nao_e_montado(client):
    """O `client` do `conftest.py` (Marco 6) chama `create_app` sem `publish_catalog` —
    `/internal/catalog/*` precisa dar 404, não 401: a rota simplesmente não existe."""
    response = client.post("/internal/catalog/reload")

    assert response.status_code == 404
