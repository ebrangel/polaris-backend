"""`SQLAlchemyQueryExecutor` contra um Postgres real (testcontainers) — prova que o SQL
compilado no Marco 5 (ver `test_sql_builder.py`) de fato executa e devolve dados certos.

Cobre modelo plano/agregado (`vendas_agregado_uf`, seção 1.0) e star schema com dois
`JOIN` — uma versão **Postgres** do desenho de `vendas_detalhado`, criada só neste
módulo: `vendas_detalhado` é Oracle nos fixtures compartilhados, e Oracle real está
fora de escopo deste marco (imagem pesada/lenta — decisão registrada no plano).
"""

import shutil
import subprocess

import pytest
from fixtures import vendas_agregado_uf, vendas_schema
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.community.postgres import PostgresContainer

from adapters.executors.sqlalchemy_executor import SQLAlchemyQueryExecutor
from domain.errors import QueryTimeoutError
from domain.models import (
    Aggregation,
    Column,
    ColumnMapping,
    Dataset,
    Datasource,
    DatasourceType,
    DataType,
    DimensionTable,
    Fact,
    FactKey,
    Filter,
    FilterOperator,
    Join,
    OrderBy,
    Provides,
    QueryRequest,
    QueryStatus,
    SortDirection,
    StarModel,
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


def _vendas_detalhado_postgres() -> Dataset:
    """Mesmo desenho de `vendas_detalhado` (seção 1.0), mas Postgres."""
    return Dataset(
        name="vendas_detalhado_pg",
        datasource=Datasource(type=DatasourceType.POSTGRES, connection_ref="env:TEST_PG_URL"),
        provides=Provides(
            dimensions={"sigla_uf", "cargo"}, measures={"valor_total", "quantidade"}
        ),
        model=StarModel(
            fact=Fact(
                table="ft_vendas",
                mapping={
                    "valor_total": ColumnMapping(column="vl_total", agg=Aggregation.SUM),
                    "quantidade": ColumnMapping(column="qt_item", agg=Aggregation.SUM),
                },
                keys={
                    "cliente_id": FactKey(column="cd_cliente", references="dim_cliente.id"),
                    "cargo_id": FactKey(column="cd_cargo", references="dim_cargo.id"),
                },
            ),
            dimension_tables={
                "dim_cliente": DimensionTable(
                    table="dm_cliente",
                    primary_key="cd_cliente",
                    mapping={"sigla_uf": ColumnMapping(column="sg_uf")},
                ),
                "dim_cargo": DimensionTable(
                    table="dm_cargo",
                    primary_key="cd_cargo",
                    mapping={"cargo": ColumnMapping(column="ds_cargo")},
                ),
            },
            joins=(
                Join(from_ref="fato_vendas.cliente_id", to_ref="dim_cliente.id"),
                Join(from_ref="fato_vendas.cargo_id", to_ref="dim_cargo.id"),
            ),
        ),
    )


def _slow_dataset() -> Dataset:
    """Aponta para uma view cujo `SELECT` interno chama `pg_sleep` — força um timeout
    real sem precisar de suporte especial no query builder."""
    return Dataset(
        name="vendas_lenta",
        datasource=Datasource(type=DatasourceType.POSTGRES, connection_ref="env:TEST_PG_URL"),
        provides=Provides(dimensions={"uf"}, measures={"valor_total"}),
        model=TableModel(
            source="dw.vendas_lenta",
            mapping={
                "uf": ColumnMapping(column="uf"),
                "valor_total": ColumnMapping(column="vl_total", agg=Aggregation.SUM),
            },
        ),
    )


_DDL = """
CREATE SCHEMA IF NOT EXISTS dw;

CREATE TABLE dw.vendas_agregado_uf (uf text, vl_total double precision, qt_total integer);
INSERT INTO dw.vendas_agregado_uf (uf, vl_total, qt_total) VALUES
    ('SP', 458320.50, 1204),
    ('RJ', 212904.10, 588),
    ('MG', 150000.00, 300);

CREATE VIEW dw.vendas_lenta AS
    SELECT 'x' AS uf, 1.0 AS vl_total,
           (SELECT 1 FROM (SELECT pg_sleep(2)) AS _s) AS _delay;

CREATE TABLE dm_cliente (cd_cliente int PRIMARY KEY, sg_uf text);
CREATE TABLE dm_cargo (cd_cargo int PRIMARY KEY, ds_cargo text);
CREATE TABLE ft_vendas (cd_cliente int, cd_cargo int, vl_total double precision, qt_item integer);

INSERT INTO dm_cliente (cd_cliente, sg_uf) VALUES (1, 'SP'), (2, 'RJ');
INSERT INTO dm_cargo (cd_cargo, ds_cargo) VALUES (10, 'ANALISTA'), (20, 'GERENTE');
INSERT INTO ft_vendas (cd_cliente, cd_cargo, vl_total, qt_item) VALUES
    (1, 10, 1000.00, 5),
    (1, 20, 2000.00, 3),
    (2, 10, 500.00, 2);
"""


async def _apply_ddl(url: str) -> None:
    engine = create_async_engine(url)
    async with engine.begin() as conn:
        for statement in _DDL.strip().split(";"):
            statement = statement.strip()
            if statement:
                await conn.execute(text(statement))
    await engine.dispose()


@pytest.fixture(scope="module")
def pg_url():
    """Container único para o módulo; o DDL é aplicado uma vez via `asyncio.run` (loop
    próprio, fechado logo em seguida) — cada teste depois cria seu próprio engine
    (`pg_engine`), preso ao event loop daquele teste especificamente."""
    import asyncio

    with PostgresContainer("postgres:16-alpine", driver="psycopg") as pg:
        url = pg.get_connection_url()
        asyncio.run(_apply_ddl(url))
        yield url


@pytest.fixture
async def pg_engine(pg_url):
    engine = create_async_engine(pg_url)
    yield engine
    await engine.dispose()


async def test_modelo_plano_com_filtro_e_ordenacao_da_secao_2_2(pg_engine):
    """A requisição da seção 2.2, executada de verdade."""
    schema = vendas_schema()
    dataset = vendas_agregado_uf()
    executor = SQLAlchemyQueryExecutor(engine=pg_engine)
    request = QueryRequest(
        schema="vendas",
        dimensions=("sigla_uf",),
        measures=("valor_total", "quantidade"),
        filters=(Filter(field="sigla_uf", operator=FilterOperator.IN, value=["SP", "RJ"]),),
        order_by=(OrderBy(field="valor_total", direction=SortDirection.DESC),),
    )
    columns = schema.columns_for(request)

    result = await executor.execute(dataset, request, columns)

    assert result.status is QueryStatus.COMPLETED
    assert result.rows == (("SP", 458320.50, 1204), ("RJ", 212904.10, 588))
    assert result.meta.dataset_used == "vendas_agregado_uf"
    assert result.meta.row_count == 2


async def test_limit_e_offset_reais(pg_engine):
    schema = vendas_schema()
    dataset = vendas_agregado_uf()
    executor = SQLAlchemyQueryExecutor(engine=pg_engine)
    request = QueryRequest(
        schema="vendas",
        dimensions=("sigla_uf",),
        measures=("valor_total",),
        order_by=(OrderBy(field="valor_total", direction=SortDirection.DESC),),
        limit=2,
        offset=1,
    )
    columns = schema.columns_for(request)

    result = await executor.execute(dataset, request, columns)

    assert result.rows == (("RJ", 212904.10), ("MG", 150000.00))


async def test_star_schema_com_join_real(pg_engine):
    dataset = _vendas_detalhado_postgres()
    executor = SQLAlchemyQueryExecutor(engine=pg_engine)
    request = QueryRequest(
        schema="vendas",
        dimensions=("sigla_uf", "cargo"),
        measures=("valor_total", "quantidade"),
        filters=(Filter(field="cargo", operator=FilterOperator.EQ, value="ANALISTA"),),
        order_by=(OrderBy(field="sigla_uf", direction=SortDirection.ASC),),
    )
    columns = (
        Column(field="sigla_uf", type=DataType.STRING),
        Column(field="cargo", type=DataType.STRING),
        Column(field="valor_total", type=DataType.NUMBER),
        Column(field="quantidade", type=DataType.NUMBER),
    )

    result = await executor.execute(dataset, request, columns)

    assert result.rows == (
        ("RJ", "ANALISTA", 500.0, 2),
        ("SP", "ANALISTA", 1000.0, 5),
    )


async def test_timeout_real_vira_query_timeout_error(pg_engine):
    executor = SQLAlchemyQueryExecutor(engine=pg_engine, timeout_seconds=0.5)
    dataset = _slow_dataset()
    request = QueryRequest(schema="vendas", dimensions=("uf",), measures=("valor_total",))
    columns = (
        Column(field="uf", type=DataType.STRING),
        Column(field="valor_total", type=DataType.NUMBER),
    )

    with pytest.raises(QueryTimeoutError):
        await executor.execute(dataset, request, columns)
