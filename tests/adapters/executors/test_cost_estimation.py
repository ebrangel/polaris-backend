"""`SQLAlchemyQueryExecutor.estimate_cost` contra um Postgres real — `EXPLAIN (FORMAT
JSON)` de verdade (`docs/escalabilidade.md`: "EXPLAIN PLAN/profile prévio, quando o
datasource suportar"). O caso Oracle/dialeto-não-suportado (sem I/O) está em
`test_sqlalchemy_executor.py`.
"""

import shutil
import subprocess

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.community.postgres import PostgresContainer

from adapters.executors.sqlalchemy_executor import SQLAlchemyQueryExecutor
from domain.models import (
    Aggregation,
    ColumnMapping,
    Dataset,
    Datasource,
    DatasourceType,
    Filter,
    FilterOperator,
    Provides,
    QueryRequest,
    TableModel,
)

pytestmark = pytest.mark.integration


def _docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        subprocess.run(["docker", "info"], check=True, capture_output=True, timeout=5)
        return True
    except Exception:
        return False


if not _docker_available():
    pytest.skip("Docker indisponível — pulando testes de integração", allow_module_level=True)


def _dataset() -> Dataset:
    return Dataset(
        name="vendas_por_uf",
        datasource=Datasource(type=DatasourceType.POSTGRES, connection_ref="env:TEST_PG_URL"),
        provides=Provides(dimensions={"uf"}, measures={"total"}),
        model=TableModel(
            source="dw.vendas_por_uf",
            mapping={
                "uf": ColumnMapping(column="uf"),
                "total": ColumnMapping(column="total", agg=Aggregation.SUM),
            },
        ),
    )


_DDL = """
CREATE SCHEMA dw;
CREATE TABLE dw.vendas_por_uf (uf text, total numeric);
INSERT INTO dw.vendas_por_uf
    SELECT (ARRAY['SP', 'RJ', 'MG'])[1 + (g % 3)], g
    FROM generate_series(1, 20000) g;
ANALYZE dw.vendas_por_uf;
"""


@pytest.fixture(scope="module")
def pg_url():
    import asyncio

    async def apply_ddl(url: str) -> None:
        engine = create_async_engine(url)
        async with engine.begin() as conn:
            for statement in _DDL.strip().split(";"):
                statement = statement.strip()
                if statement:
                    await conn.execute(text(statement))
        await engine.dispose()

    with PostgresContainer("postgres:16-alpine", driver="psycopg") as pg:
        url = pg.get_connection_url()
        asyncio.run(apply_ddl(url))
        yield url


@pytest.fixture
async def executor(pg_url):
    engine = create_async_engine(pg_url)
    yield SQLAlchemyQueryExecutor(light_engine=engine, heavy_engine=engine)
    await engine.dispose()


async def test_explain_real_devolve_score_positivo(executor):
    request = QueryRequest(schema="vendas", dimensions=("uf",), measures=("total",))

    cost = await executor.estimate_cost(_dataset(), request)

    assert cost.score > 0
    assert "EXPLAIN" in cost.detail


async def test_filtro_seletivo_reduz_o_score_estimado(executor):
    """O planner do Postgres estima menos linhas (logo, menos custo) quando o `WHERE`
    restringe o resultado — sem precisar de índice, só das estatísticas do `ANALYZE`."""
    dataset = _dataset()
    sem_filtro = QueryRequest(schema="vendas", dimensions=("uf",), measures=("total",))
    com_filtro = QueryRequest(
        schema="vendas",
        dimensions=("uf",),
        measures=("total",),
        filters=(Filter(field="uf", operator=FilterOperator.EQ, value="SP"),),
    )

    custo_sem_filtro = await executor.estimate_cost(dataset, sem_filtro)
    custo_com_filtro = await executor.estimate_cost(dataset, com_filtro)

    assert custo_com_filtro.score < custo_sem_filtro.score


async def test_threshold_configurado_decide_is_heavy(pg_url):
    engine = create_async_engine(pg_url)
    executor_barato = SQLAlchemyQueryExecutor(
        light_engine=engine, heavy_engine=engine, cost_threshold=1.0
    )
    executor_caro = SQLAlchemyQueryExecutor(
        light_engine=engine, heavy_engine=engine, cost_threshold=1_000_000.0
    )
    request = QueryRequest(schema="vendas", dimensions=("uf",), measures=("total",))

    barato = await executor_barato.estimate_cost(_dataset(), request)
    caro = await executor_caro.estimate_cost(_dataset(), request)

    assert barato.is_heavy  # mesmo score, limiar baixo
    assert not caro.is_heavy  # mesmo score, limiar alto
    assert barato.score == caro.score

    await engine.dispose()
