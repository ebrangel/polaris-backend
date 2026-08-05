"""Parsing dos parâmetros planos do `GET /v1/query` (seção 2.2a, opção B) — puro, sem HTTP.

O exemplo canônico do documento:

```
?schema=vendas&dimensions=sigla_uf,cargo&measures=valor_total
&filter[sigla_uf][in]=SP,RJ&order_by=valor_total.desc&limit=100&offset=0
```
"""

from datetime import date

import pytest
from fixtures import vendas_schema

from adapters.api.query_params import parse_flat_params, schema_name_from_params
from domain.errors import InvalidFilterError, UnknownFieldError
from domain.models import (
    Aggregation,
    ColumnMapping,
    Dataset,
    Datasource,
    DatasourceType,
    DataType,
    Dimension,
    FilterOperator,
    Measure,
    Provides,
    Schema,
    SortDirection,
    TableModel,
)

#: O exemplo literal da seção 2.2a, já decomposto em pares chave/valor.
EXEMPLO_SECAO_2_2A = {
    "schema": "vendas",
    "dimensions": "sigla_uf,cargo",
    "measures": "valor_total",
    "filter[sigla_uf][in]": "SP,RJ",
    "order_by": "valor_total.desc",
    "limit": "100",
    "offset": "0",
}


def test_exemplo_da_secao_2_2a():
    model = parse_flat_params(EXEMPLO_SECAO_2_2A, vendas_schema())

    assert model.schema_name == "vendas"
    assert model.dimensions == ["sigla_uf", "cargo"]
    assert model.measures == ["valor_total"]
    assert len(model.filters) == 1
    assert model.filters[0].field == "sigla_uf"
    assert model.filters[0].operator is FilterOperator.IN
    assert model.filters[0].value == ["SP", "RJ"]
    assert model.order_by[0].field == "valor_total"
    assert model.order_by[0].direction is SortDirection.DESC
    assert model.limit == 100
    assert model.offset == 0


def test_converge_para_o_mesmo_query_request_do_post():
    """O ponto do marco: as duas rotas produzem a mesma entidade de domínio."""
    from adapters.api.schemas import QueryRequestModel

    do_get = parse_flat_params(EXEMPLO_SECAO_2_2A, vendas_schema()).to_domain()
    do_post = QueryRequestModel.model_validate(
        {
            "schema": "vendas",
            "dimensions": ["sigla_uf", "cargo"],
            "measures": ["valor_total"],
            "filters": [{"field": "sigla_uf", "operator": "in", "value": ["SP", "RJ"]}],
            "order_by": [{"field": "valor_total", "direction": "desc"}],
            "limit": 100,
            "offset": 0,
        }
    ).to_domain()

    assert do_get == do_post
    assert do_get.query_id == do_post.query_id


def test_multiplos_filtros_em_campos_diferentes_nao_colidem():
    """"cada campo diferente é uma chave distinta, então múltiplos filtros não colidem"."""
    params = {
        "schema": "vendas",
        "dimensions": "sigla_uf",
        "filter[sigla_uf][in]": "SP,RJ",
        "filter[cargo][eq]": "ANALISTA",
    }

    model = parse_flat_params(params, vendas_schema())

    por_campo = {f.field: f for f in model.filters}
    assert por_campo["sigla_uf"].value == ["SP", "RJ"]
    assert por_campo["cargo"].operator is FilterOperator.EQ
    assert por_campo["cargo"].value == "ANALISTA"


def test_ordenacao_multipla_com_direcoes_distintas():
    params = {"schema": "vendas", "dimensions": "sigla_uf", "order_by": "valor_total.desc,sigla_uf.asc"}

    model = parse_flat_params(params, vendas_schema())

    assert [(o.field, o.direction.value) for o in model.order_by] == [
        ("valor_total", "desc"),
        ("sigla_uf", "asc"),
    ]


def test_ordenacao_sem_direcao_assume_asc():
    params = {"schema": "vendas", "dimensions": "sigla_uf", "order_by": "sigla_uf"}

    model = parse_flat_params(params, vendas_schema())

    assert model.order_by[0].direction is SortDirection.ASC


def test_direcao_invalida_e_erro_de_filtro():
    params = {"schema": "vendas", "dimensions": "sigla_uf", "order_by": "sigla_uf.acima"}

    with pytest.raises(InvalidFilterError, match="asc"):
        parse_flat_params(params, vendas_schema())


def test_operador_inexistente():
    params = {"schema": "vendas", "dimensions": "sigla_uf", "filter[sigla_uf][aproximado]": "SP"}

    with pytest.raises(InvalidFilterError, match="aproximado"):
        parse_flat_params(params, vendas_schema())


def test_filtro_sobre_campo_inexistente():
    params = {"schema": "vendas", "dimensions": "sigla_uf", "filter[canal][eq]": "web"}

    with pytest.raises(UnknownFieldError) as excinfo:
        parse_flat_params(params, vendas_schema())

    assert excinfo.value.fields == ("canal",)


def test_ausencia_de_schema():
    with pytest.raises(UnknownFieldError):
        schema_name_from_params({"dimensions": "sigla_uf"})


def test_limit_nao_inteiro():
    params = {"schema": "vendas", "dimensions": "sigla_uf", "limit": "muitos"}

    with pytest.raises(InvalidFilterError, match="inteiro"):
        parse_flat_params(params, vendas_schema())


# --- Coerção de tipo: querystring é sempre texto -----------------------------------------


def _schema_tipado() -> Schema:
    """Dimensões `number`/`boolean`/`date` — nenhum schema do documento tem, e sem
    coerção o driver receberia texto onde o banco espera número/data."""
    dataset = Dataset(
        name="metricas",
        datasource=Datasource(type=DatasourceType.POSTGRES, connection_ref="env:X"),
        provides=Provides(dimensions={"ano", "ativo", "dia"}, measures={"total"}),
        model=TableModel(
            source="dw.metricas",
            mapping={
                "ano": ColumnMapping(column="ano"),
                "ativo": ColumnMapping(column="ativo"),
                "dia": ColumnMapping(column="dia"),
                "total": ColumnMapping(column="total", agg=Aggregation.SUM),
            },
        ),
    )
    return Schema(
        name="metricas",
        version=1,
        dimensions={
            "ano": Dimension(name="ano", type=DataType.NUMBER),
            "ativo": Dimension(name="ativo", type=DataType.BOOLEAN),
            "dia": Dimension(name="dia", type=DataType.DATE),
        },
        measures={"total": Measure(name="total", agg=Aggregation.SUM)},
        datasets=(dataset,),
    )


@pytest.mark.parametrize(
    "key, raw, expected",
    [
        ("filter[ano][gt]", "2020", 2020),
        ("filter[ano][eq]", "2020.5", 2020.5),
        ("filter[ativo][eq]", "true", True),
        ("filter[ativo][eq]", "false", False),
        ("filter[dia][gte]", "2024-01-31", date(2024, 1, 31)),
    ],
)
def test_coercao_por_tipo_da_dimensao(key, raw, expected):
    params = {"schema": "metricas", "dimensions": "ano", key: raw}

    model = parse_flat_params(params, _schema_tipado())

    assert model.filters[0].value == expected


def test_coercao_de_lista_em_between():
    params = {"schema": "metricas", "dimensions": "ano", "filter[ano][between]": "2020,2024"}

    model = parse_flat_params(params, _schema_tipado())

    assert model.filters[0].value == [2020, 2024]


def test_valor_incoercivel_vira_invalid_filter():
    params = {"schema": "metricas", "dimensions": "ano", "filter[ano][gt]": "ontem"}

    with pytest.raises(InvalidFilterError) as excinfo:
        parse_flat_params(params, _schema_tipado())

    assert excinfo.value.fields == ("ano",)


def test_dimensao_string_nao_e_coagida():
    """`sigla_uf` é `string`: um valor que parece número continua texto."""
    params = {"schema": "vendas", "dimensions": "sigla_uf", "filter[sigla_uf][eq]": "01234"}

    model = parse_flat_params(params, vendas_schema())

    assert model.filters[0].value == "01234"
