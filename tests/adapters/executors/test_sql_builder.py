"""`build_select` — compilação de SQL, sem rede nem servidor.

Confere os dois dialetos relacionais citados no CLAUDE.md (postgresql, oracle) a partir
dos exemplos de `docs/catalogo-e-contrato-completo.md`: `estoque_atual_pg` (seção 1.2,
modelo plano) e `vendas_detalhado` (seção 1.0, star schema em Oracle). Cobertura de
Oracle fica só neste nível — não há Oracle real nos testes de integração (Marco 5,
decisão registrada no plano: imagem pesada, licenciamento, start lento).
"""

import pytest
from fixtures import (
    estoque_atual_pg,
    estoque_schema,
    vendas_agregado_uf,
    vendas_detalhado,
    vendas_schema,
)
from sqlalchemy.dialects import oracle, postgresql

from adapters.executors.sql_builder import build_select
from domain.models import (
    Aggregation,
    ColumnMapping,
    Dataset,
    Datasource,
    DatasourceType,
    DataType,
    Dimension,
    Filter,
    FilterOperator,
    Measure,
    OrderBy,
    Provides,
    QueryRequest,
    Schema,
    SortDirection,
    TableModel,
)


def _compile(stmt, dialect) -> str:
    return str(stmt.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))


# --- Modelo plano (seção 1.2) — sem JOIN --------------------------------------------------


def test_modelo_plano_compila_sem_join():
    schema = estoque_schema()
    dataset = estoque_atual_pg()
    request = QueryRequest(
        schema="estoque",
        dimensions=("filial",),
        measures=("quantidade_disponivel",),
        limit=10,
        offset=5,
    )
    columns = schema.columns_for(request)

    sql = _compile(build_select(dataset, request, columns), postgresql.dialect())

    assert "FROM app.vw_estoque_atual" in sql
    assert "JOIN" not in sql
    assert "app.vw_estoque_atual.filial AS filial" in sql
    assert "sum(app.vw_estoque_atual.qtd_disponivel) AS quantidade_disponivel" in sql
    assert "GROUP BY app.vw_estoque_atual.filial" in sql
    assert "LIMIT 10" in sql
    assert "OFFSET 5" in sql


def test_sem_medida_pedida_nao_tem_group_by():
    schema = estoque_schema()
    dataset = estoque_atual_pg()
    request = QueryRequest(schema="estoque", dimensions=("filial", "produto"))
    columns = schema.columns_for(request)

    sql = _compile(build_select(dataset, request, columns), postgresql.dialect())

    assert "GROUP BY" not in sql


# --- Star schema (seção 1.0) — dois JOIN, dialeto Oracle -----------------------------------


def test_star_schema_compila_os_dois_joins_em_oracle():
    schema = vendas_schema()
    dataset = vendas_detalhado()
    request = QueryRequest(
        schema="vendas",
        dimensions=("sigla_uf", "cargo"),
        measures=("valor_total", "quantidade"),
        filters=(Filter(field="sigla_uf", operator=FilterOperator.IN, value=["SP", "RJ"]),),
        order_by=(OrderBy(field="valor_total", direction=SortDirection.DESC),),
    )
    columns = schema.columns_for(request)

    sql = _compile(build_select(dataset, request, columns), oracle.dialect())

    assert 'FROM "SCHEMA_DW"."FT_VENDAS"' in sql
    assert (
        'JOIN "SCHEMA_DW"."DM_CLIENTE" ON "SCHEMA_DW"."FT_VENDAS"."CD_CLIENTE" = '
        '"SCHEMA_DW"."DM_CLIENTE"."CD_CLIENTE"' in sql
    )
    assert (
        'JOIN "SCHEMA_DW"."DM_CARGO" ON "SCHEMA_DW"."FT_VENDAS"."CD_CARGO" = '
        '"SCHEMA_DW"."DM_CARGO"."CD_CARGO"' in sql
    )
    assert 'sum("SCHEMA_DW"."FT_VENDAS"."VL_TOTAL") AS valor_total' in sql
    assert 'sum("SCHEMA_DW"."FT_VENDAS"."QT_ITEM") AS quantidade' in sql
    assert 'WHERE "SCHEMA_DW"."DM_CLIENTE"."SG_UF" IN (\'SP\', \'RJ\')' in sql
    assert 'GROUP BY "SCHEMA_DW"."DM_CLIENTE"."SG_UF", "SCHEMA_DW"."DM_CARGO"."DS_CARGO"' in sql


def test_ordenar_por_medida_usa_a_expressao_agregada():
    """O exemplo da seção 2.2 ordena por `valor_total`, que é medida, não dimensão."""
    schema = vendas_schema()
    dataset = vendas_detalhado()
    request = QueryRequest(
        schema="vendas",
        dimensions=("sigla_uf",),
        measures=("valor_total",),
        order_by=(OrderBy(field="valor_total", direction=SortDirection.DESC),),
    )
    columns = schema.columns_for(request)

    sql = _compile(build_select(dataset, request, columns), oracle.dialect())

    assert 'ORDER BY sum("SCHEMA_DW"."FT_VENDAS"."VL_TOTAL") DESC' in sql


def test_ordenar_por_dimensao():
    schema = vendas_schema()
    dataset = vendas_detalhado()
    request = QueryRequest(
        schema="vendas",
        dimensions=("sigla_uf",),
        measures=("valor_total",),
        order_by=(OrderBy(field="sigla_uf", direction=SortDirection.ASC),),
    )
    columns = schema.columns_for(request)

    sql = _compile(build_select(dataset, request, columns), oracle.dialect())

    assert 'ORDER BY "SCHEMA_DW"."DM_CLIENTE"."SG_UF" ASC' in sql


# --- Operadores de filtro (seção 2.2, os 9) -------------------------------------------------


@pytest.mark.parametrize(
    "operator, value, expected",
    [
        (FilterOperator.EQ, "SP", "uf = 'SP'"),
        (FilterOperator.NEQ, "SP", "uf != 'SP'"),
        (FilterOperator.IN, ["SP", "RJ"], "uf IN ('SP', 'RJ')"),
    ],
)
def test_operadores_validos_para_string(operator, value, expected):
    schema = vendas_schema()
    dataset = vendas_agregado_uf()
    request = QueryRequest(
        schema="vendas",
        dimensions=("sigla_uf",),
        measures=("valor_total",),
        filters=(Filter(field="sigla_uf", operator=operator, value=value),),
    )
    columns = schema.columns_for(request)

    sql = _compile(build_select(dataset, request, columns), postgresql.dialect())

    assert expected in sql


def test_operador_contains_vira_like():
    schema = vendas_schema()
    dataset = vendas_agregado_uf()
    request = QueryRequest(
        schema="vendas",
        dimensions=("sigla_uf",),
        measures=("valor_total",),
        filters=(Filter(field="sigla_uf", operator=FilterOperator.CONTAINS, value="S"),),
    )
    columns = schema.columns_for(request)

    sql = _compile(build_select(dataset, request, columns), postgresql.dialect())

    assert "uf LIKE" in sql
    assert "'S'" in sql


def _numeric_schema_and_dataset() -> tuple[Schema, Dataset]:
    """Nenhum schema de `tests/fixtures.py` tem dimensão numérica filtrável — os
    exemplos do documento só usam `string`. Sintético, local a este módulo, só para
    testar `gt`/`gte`/`lt`/`lte`/`between`, que a seção 2.2 restringe a tipos não-string."""
    dataset = Dataset(
        name="pedidos_por_ano",
        datasource=Datasource(type=DatasourceType.POSTGRES, connection_ref="env:TEST_PG_URL"),
        provides=Provides(dimensions={"ano"}, measures={"total"}),
        model=TableModel(
            source="analytics.pedidos_ano",
            mapping={
                "ano": ColumnMapping(column="ano"),
                "total": ColumnMapping(column="qtd_total", agg=Aggregation.SUM),
            },
        ),
    )
    schema = Schema(
        name="pedidos",
        version=1,
        dimensions={"ano": Dimension(name="ano", type=DataType.NUMBER, filterable=True)},
        measures={"total": Measure(name="total", agg=Aggregation.SUM)},
        datasets=(dataset,),
    )
    return schema, dataset


@pytest.mark.parametrize(
    "operator, value, expected",
    [
        (FilterOperator.GT, 2020, "ano > 2020"),
        (FilterOperator.GTE, 2020, "ano >= 2020"),
        (FilterOperator.LT, 2024, "ano < 2024"),
        (FilterOperator.LTE, 2024, "ano <= 2024"),
        (FilterOperator.BETWEEN, [2020, 2024], "ano BETWEEN 2020 AND 2024"),
    ],
)
def test_operadores_numericos(operator, value, expected):
    schema, dataset = _numeric_schema_and_dataset()
    request = QueryRequest(
        schema="pedidos",
        dimensions=("ano",),
        measures=("total",),
        filters=(Filter(field="ano", operator=operator, value=value),),
    )
    columns = schema.columns_for(request)

    sql = _compile(build_select(dataset, request, columns), postgresql.dialect())

    assert expected in sql
