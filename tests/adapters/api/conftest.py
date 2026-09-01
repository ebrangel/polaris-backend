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
from fakes import (
    InMemoryCacheGateway,
    InMemoryJobQueue,
    InMemoryResultExporter,
    StubQueryExecutor,
)

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
def exporter() -> InMemoryResultExporter:
    return InMemoryResultExporter()


def _build_client(catalog, executor, cache, job_queue, exporter) -> TestClient:
    execute_query = ExecuteQuery(
        catalog=catalog,
        resolve_dataset=ResolveDataset(),
        cache=cache,
        job_queue=job_queue,
    )
    app = create_app(
        catalog=catalog,
        execute_query=execute_query,
        job_queue=job_queue,
        result_exporter=exporter,
    )
    return TestClient(app)


@pytest.fixture
def client(catalog, executor, cache, job_queue) -> TestClient:
    """O mesmo `StubQueryExecutor` atende as três engines: qual executor roda não é
    assunto desta camada (é testado em `tests/application/test_execute_query.py`).

    **Sem exportador** — é o servidor mínimo do Marco 6, e o que garante que o caminho
    de consulta continua inteiro sem o export da seção 2.4a. Quem precisa de export usa
    a fixture `client_com_export`.
    """
    return _build_client(catalog, executor, cache, job_queue, exporter=None)


@pytest.fixture
def client_com_export(catalog, executor, cache, job_queue, exporter) -> TestClient:
    """App com `ResultExporter` injetado — o cenário de produção (seção 2.4a)."""
    return _build_client(catalog, executor, cache, job_queue, exporter)


@pytest.fixture
def financeiro() -> dict[str, str]:
    """Header de roles que libera as medidas do schema `vendas` (seção 1.0)."""
    return {"X-Roles": "financeiro"}
