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
from fakes import CollectingRowSink
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
    sink = CollectingRowSink()

    streamed = await executor.execute(dataset, request, columns, sink)

    assert tuple(sink.rows) == (("SP", 458320.50, 1204), ("RJ", 212904.10, 588))
    assert streamed.row_count == 2
    assert streamed.total_rows == 2


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
    sink = CollectingRowSink()

    streamed = await executor.execute(dataset, request, columns, sink)

    assert tuple(sink.rows) == (("RJ", 212904.10), ("MG", 150000.00))
    # `offset > 0` -> a janela entra no SELECT e o total sai na mesma passada. São 3 UFs
    # na fixture (SP, RJ, MG); o `limit=2` devolve duas, e `total_rows` não se confunde
    # com isso — é o número de linhas antes de `limit`/`offset`.
    assert streamed.row_count == 2
    assert streamed.total_rows == 3


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

    sink = CollectingRowSink()

    await executor.execute(dataset, request, columns, sink)

    assert tuple(sink.rows) == (
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
        await executor.execute(dataset, request, columns, CollectingRowSink())


# --- Leitura em blocos e contador total (Marco 12) -----------------------------------------

#: 20 mil grupos distintos. O volume é o que dá contraste ao teste de memória: com
#: poucos milhares de linhas, o pico de uma versão que materializa se perde no ruído do
#: driver, e o teste passaria mesmo com a regressão que ele existe para pegar.
_GRUPOS = 20_000

_DDL_MUITAS_LINHAS = f"""
CREATE TABLE IF NOT EXISTS ft_muitas AS
SELECT
    'UF' || lpad((i % {_GRUPOS})::text, 6, '0') AS uf,
    (i * 1.5)::numeric AS vl
FROM generate_series(1, {_GRUPOS * 2}) AS s(i);
"""


def _dataset_muitas_linhas() -> Dataset:
    return Dataset(
        name="muitas_linhas",
        datasource=Datasource(type=DatasourceType.POSTGRES, connection_ref="env:TEST_PG_URL"),
        provides=Provides(dimensions={"uf"}, measures={"valor_total"}),
        model=TableModel(
            source="ft_muitas",
            mapping={
                "uf": ColumnMapping(column="uf"),
                "valor_total": ColumnMapping(column="vl", agg=Aggregation.SUM),
            },
        ),
    )


_COLUNAS_MUITAS = (
    Column(field="uf", type=DataType.STRING),
    Column(field="valor_total", type=DataType.NUMBER),
)


@pytest.fixture
async def pg_muitas(pg_engine):
    async with pg_engine.begin() as conn:
        await conn.execute(text(_DDL_MUITAS_LINHAS))
    return pg_engine


async def test_le_o_cursor_em_blocos_do_tamanho_configurado(pg_muitas):
    """O ponto do marco: o resultado chega ao destino em lotes, e não de uma vez.

    Com `chunk_size=100`, os 20 mil grupos chegam em 200 blocos — e em nenhum momento as
    20 mil linhas estão juntas dentro do executor.
    """
    executor = SQLAlchemyQueryExecutor(engine=pg_muitas, chunk_size=100)
    request = QueryRequest(schema="vendas", dimensions=("uf",), measures=("valor_total",))
    sink = CollectingRowSink()

    streamed = await executor.execute(
        _dataset_muitas_linhas(), request, _COLUNAS_MUITAS, sink
    )

    assert streamed.row_count == _GRUPOS
    assert len(sink.chunks) == _GRUPOS // 100
    assert all(len(chunk) == 100 for chunk in sink.chunks)


async def test_a_coluna_da_janela_nunca_chega_ao_sink(pg_muitas):
    """`COUNT(*) OVER ()` acrescenta uma coluna ao SELECT; ela é metadado, não resultado,
    e sair no CSV ou no JSON seria um campo que o cliente nunca pediu."""
    executor = SQLAlchemyQueryExecutor(engine=pg_muitas, chunk_size=100)
    request = QueryRequest(
        schema="vendas", dimensions=("uf",), measures=("valor_total",), limit=10, offset=5
    )
    sink = CollectingRowSink()

    streamed = await executor.execute(
        _dataset_muitas_linhas(), request, _COLUNAS_MUITAS, sink
    )

    assert all(len(row) == 2 for row in sink.rows)
    assert streamed.row_count == 10
    assert streamed.total_rows == _GRUPOS  # o total antes de limit/offset


async def test_sem_offset_e_abaixo_do_limite_o_total_sai_de_graca(pg_muitas):
    """Caso comum: o resultado coube inteiro, então `total_rows == row_count` e nenhuma
    contagem extra é pedida ao banco."""
    executor = SQLAlchemyQueryExecutor(engine=pg_muitas, chunk_size=1000)
    request = QueryRequest(
        schema="vendas", dimensions=("uf",), measures=("valor_total",), limit=_GRUPOS * 2
    )
    sink = CollectingRowSink()

    streamed = await executor.execute(
        _dataset_muitas_linhas(), request, _COLUNAS_MUITAS, sink
    )

    assert streamed.row_count == _GRUPOS
    assert streamed.total_rows == _GRUPOS


async def test_sem_offset_mas_truncado_o_total_vem_da_contagem_de_apoio(pg_muitas):
    """Bateu no `limit`: pode haver mais, e isso só se sabe depois de drenar — tarde
    demais para uma coluna de janela. Aí sai o `SELECT count(*) FROM (...)`."""
    executor = SQLAlchemyQueryExecutor(engine=pg_muitas, chunk_size=1000)
    request = QueryRequest(
        schema="vendas", dimensions=("uf",), measures=("valor_total",), limit=10
    )
    sink = CollectingRowSink()

    streamed = await executor.execute(
        _dataset_muitas_linhas(), request, _COLUNAS_MUITAS, sink
    )

    assert streamed.row_count == 10
    assert streamed.total_rows == _GRUPOS


async def test_offset_alem_do_fim_ainda_informa_o_total(pg_muitas):
    """Zero linhas significa nenhuma janela para ler — o total existe e vem do plano B."""
    executor = SQLAlchemyQueryExecutor(engine=pg_muitas, chunk_size=1000)
    request = QueryRequest(
        schema="vendas", dimensions=("uf",), measures=("valor_total",), limit=10, offset=_GRUPOS * 2
    )
    sink = CollectingRowSink()

    streamed = await executor.execute(
        _dataset_muitas_linhas(), request, _COLUNAS_MUITAS, sink
    )

    assert streamed.row_count == 0
    assert streamed.total_rows == _GRUPOS


async def test_memoria_do_worker_nao_cresce_com_o_resultado(pg_muitas):
    """A verificação que dá sentido ao marco.

    Todos os testes acima passariam igual com `result.all()` — eles conferem *o quê*, não
    *como*. Este mede o pico de alocação enquanto o cursor é lido, com um sink que
    descarta as linhas: lendo em blocos o pico fica na ordem do bloco; materializando,
    cresce com o número de linhas.

    Os limiares vêm de medição, não de chute. Nesta fixture, dez vezes mais linhas dão:

        streaming     ~198 KB -> ~105 KB   (0,5x — variação, não crescimento)
        materializado ~549 KB -> ~4,6 MB   (8,4x)

    Daí o `3x` separar os dois casos com folga dos dois lados, e o teto absoluto de 1 MB
    ser uma afirmação direta: 20 mil linhas não cabem em 1 MB se alguém as juntar.
    """
    import tracemalloc

    class _DescartaTudo:
        async def write(self, rows):
            pass

        async def close(self, result):
            pass

        async def abort(self):
            pass

    async def _pico(limit: int) -> int:
        executor = SQLAlchemyQueryExecutor(engine=pg_muitas, chunk_size=100)
        request = QueryRequest(
            schema="vendas", dimensions=("uf",), measures=("valor_total",), limit=limit
        )
        tracemalloc.start()
        await executor.execute(
            _dataset_muitas_linhas(), request, _COLUNAS_MUITAS, _DescartaTudo()
        )
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        return peak

    pico_2k = await _pico(limit=2_000)
    pico_20k = await _pico(limit=_GRUPOS)

    assert pico_20k < pico_2k * 3, (
        f"o pico acompanhou o tamanho do resultado: {pico_2k} -> {pico_20k} bytes"
    )
    assert pico_20k < 1024 * 1024, (
        f"{_GRUPOS} linhas deixaram {pico_20k} bytes de pico — não estão sendo "
        "transmitidas em blocos"
    )
