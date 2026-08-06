"""`PostgresInspector` contra um Postgres real (testcontainers) — cobre modelo plano
(`estoque_atual_pg`) e star schema (fato + dimensões), tabela sem/ com schema explícito.
"""

import shutil
import subprocess

import pytest
from fixtures import estoque_atual_pg, vendas_agregado_uf
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.community.postgres import PostgresContainer

from adapters.catalog.postgres_inspector import PostgresInspector
from domain.models import (
    Aggregation,
    ColumnMapping,
    Dataset,
    Datasource,
    DatasourceType,
    DimensionTable,
    Fact,
    FactKey,
    Provides,
    StarModel,
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


_DDL = """
CREATE SCHEMA IF NOT EXISTS app;
CREATE TABLE app.vw_estoque_atual (
    filial text,
    produto text,
    qtd_disponivel numeric,
    vl_unitario numeric
);
CREATE SCHEMA IF NOT EXISTS star;
CREATE TABLE star.ft_vendas (
    vl_total numeric,
    qt_item numeric,
    cd_cliente integer
);
CREATE TABLE star.dm_cliente (
    cd_cliente integer PRIMARY KEY,
    sg_uf text
);
"""


async def _apply_ddl(url: str) -> None:
    engine = create_async_engine(url)
    async with engine.begin() as conn:
        for statement in _DDL.strip().split(";"):
            if statement.strip():
                await conn.execute(text(statement))
    await engine.dispose()


@pytest.fixture(scope="module")
def pg_url():
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


@pytest.fixture
def inspector(pg_engine) -> PostgresInspector:
    return PostgresInspector(pg_engine)


def _star_dataset_para_star_schema() -> Dataset:
    """Uma versão Postgres do star schema de `vendas_detalhado` (que é Oracle nos
    fixtures compartilhados) — só para exercitar o ramo `StarModel` do inspector."""
    return Dataset(
        name="vendas_detalhado_pg",
        datasource=Datasource(type=DatasourceType.POSTGRES, connection_ref="env:X"),
        provides=Provides(dimensions={"sigla_uf"}, measures={"valor_total", "quantidade"}),
        model=StarModel(
            fact=Fact(
                table="star.ft_vendas",
                mapping={
                    "valor_total": ColumnMapping(column="vl_total", agg=Aggregation.SUM),
                    "quantidade": ColumnMapping(column="qt_item", agg=Aggregation.SUM),
                },
                keys={"cliente_id": FactKey(column="cd_cliente", references="dim_cliente.id")},
            ),
            dimension_tables={
                "dim_cliente": DimensionTable(
                    table="star.dm_cliente",
                    primary_key="cd_cliente",
                    mapping={"sigla_uf": ColumnMapping(column="sg_uf")},
                ),
            },
        ),
    )


async def test_tabela_com_todas_as_colunas_nao_acusa_nada(inspector):
    dataset = estoque_atual_pg()

    assert await inspector.missing_fields(dataset) == ()


async def test_tabela_com_coluna_renomeada_acusa_o_campo_faltante(inspector):
    dataset = estoque_atual_pg()
    campo_quebrado = dict(dataset.model.mapping)
    campo_quebrado["filial"] = ColumnMapping(column="coluna_que_nao_existe")
    dataset_quebrado = Dataset(
        name=dataset.name,
        datasource=dataset.datasource,
        provides=dataset.provides,
        model=type(dataset.model)(source=dataset.model.source, mapping=campo_quebrado),
    )

    missing = await inspector.missing_fields(dataset_quebrado)

    assert missing == ("filial",)


async def test_star_schema_com_fato_e_dimensao_ok_nao_acusa_nada(inspector):
    dataset = _star_dataset_para_star_schema()

    assert await inspector.missing_fields(dataset) == ()


async def test_star_schema_com_coluna_da_dimensao_renomeada_acusa_o_campo(inspector):
    dataset = _star_dataset_para_star_schema()
    dim_quebrada = DimensionTable(
        table="star.dm_cliente",
        primary_key="cd_cliente",
        mapping={"sigla_uf": ColumnMapping(column="coluna_inexistente")},
    )
    dataset_quebrado = Dataset(
        name=dataset.name,
        datasource=dataset.datasource,
        provides=dataset.provides,
        model=StarModel(
            fact=dataset.model.fact,
            dimension_tables={"dim_cliente": dim_quebrada},
        ),
    )

    missing = await inspector.missing_fields(dataset_quebrado)

    assert missing == ("sigla_uf",)


async def test_dataset_de_referencia_do_fixture_dw_vendas_agregado_uf_falha_sem_a_tabela(
    inspector,
):
    """`vendas_agregado_uf` (fixture) referencia `dw.vendas_agregado_uf`, que este
    container não tem — todos os campos do mapping devem acusar falta."""
    dataset = vendas_agregado_uf()

    missing = await inspector.missing_fields(dataset)

    assert set(missing) == {"sigla_uf", "valor_total", "quantidade"}
