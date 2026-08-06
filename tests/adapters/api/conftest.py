"""Montagem do app para os testes de rota, com os fakes do Marco 2.

Nenhum banco, nenhum container: o objetivo aqui é o contrato HTTP (seções 2.1–2.6), não
a execução de consulta — essa já é coberta por `tests/adapters/executors/`.
"""

import pytest
from fastapi.testclient import TestClient
from fixtures import catalog as catalog_fixture

from adapters.api import create_app
from application.use_cases import ExecuteQuery, ResolveDataset
from domain.models import Catalog
from fakes import InMemoryCacheGateway, InMemoryJobQueue, StubQueryExecutor

#: Os quatro `connection_ref` distintos do catálogo de `catalog()` (vendas tem dois
#: Postgres diferentes — seções 1.0 e 1.2 — daí não dar para chavear por engine).
_ALL_CONNECTION_REFS = (
    "env:DW_VENDAS_PG_URL",
    "env:DW_VENDAS_ORACLE_URL",
    "env:ES_EVENTOS_URL",
    "env:APP_ESTOQUE_URL",
)


@pytest.fixture
def executor() -> StubQueryExecutor:
    return StubQueryExecutor()


@pytest.fixture
def cache() -> InMemoryCacheGateway:
    return InMemoryCacheGateway()


@pytest.fixture
def job_queue() -> InMemoryJobQueue:
    return InMemoryJobQueue()


@pytest.fixture
def catalog() -> Catalog:
    return catalog_fixture()


@pytest.fixture
def client(catalog, executor, cache, job_queue) -> TestClient:
    """O mesmo `StubQueryExecutor` atende as três engines: qual executor roda não é
    assunto desta camada (é testado em `tests/application/test_execute_query.py`)."""
    execute_query = ExecuteQuery(
        catalog=catalog,
        resolve_dataset=ResolveDataset(),
        executors=dict.fromkeys(_ALL_CONNECTION_REFS, executor),
        cache=cache,
        job_queue=job_queue,
    )
    app = create_app(catalog=catalog, execute_query=execute_query, job_queue=job_queue)
    return TestClient(app)


@pytest.fixture
def financeiro() -> dict[str, str]:
    """Header de roles que libera as medidas do schema `vendas` (seção 1.0)."""
    return {"X-Roles": "financeiro"}
