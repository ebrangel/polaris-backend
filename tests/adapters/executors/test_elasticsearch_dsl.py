"""`build_query_body`/`parse_response` — corpo da Query DSL e parsing da resposta, sem
rede nem cluster. O exemplo canônico é o da seção 1.1 (`eventos_navegacao_es`).
"""

import pytest
from fixtures import eventos_navegacao_es, eventos_schema

from adapters.executors.elasticsearch_dsl import build_query_body, parse_response
from domain.models import (
    Aggregation,
    Dataset,
    Datasource,
    DatasourceType,
    DataType,
    Dimension,
    Filter,
    FilterOperator,
    FieldMapping,
    IndexModel,
    Measure,
    OrderBy,
    Provides,
    QueryRequest,
    Schema,
    SortDirection,
)


# --- O exemplo da seção 1.1 -----------------------------------------------------------


def test_corpo_da_secao_1_1_usa_composite_com_as_duas_dimensoes():
    schema = eventos_schema()
    dataset = eventos_navegacao_es()
    request = QueryRequest(
        schema="eventos_navegacao",
        dimensions=("pais", "dispositivo"),
        measures=("duracao_media", "total_eventos"),
    )
    columns = schema.columns_for(request)

    body = build_query_body(dataset, request, columns)

    assert body == {
        "size": 0,
        "query": {"match_all": {}},
        "aggs": {
            "grouped": {
                "composite": {
                    "size": 1000,
                    "sources": [
                        {"pais": {"terms": {"field": "pais"}}},
                        {"dispositivo": {"terms": {"field": "dispositivo"}}},
                    ],
                },
                "aggs": {
                    "duracao_media": {"avg": {"field": "duracao_sessao"}},
                    "total_eventos": {"value_count": {"field": "duracao_sessao"}},
                },
            }
        },
    }


def test_zero_dimensoes_usa_agregacoes_de_topo_sem_composite():
    schema = eventos_schema()
    dataset = eventos_navegacao_es()
    request = QueryRequest(schema="eventos_navegacao", measures=("duracao_media",))
    columns = schema.columns_for(request)

    body = build_query_body(dataset, request, columns)

    assert body == {
        "size": 0,
        "query": {"match_all": {}},
        "aggs": {"duracao_media": {"avg": {"field": "duracao_sessao"}}},
    }
    assert "grouped" not in body["aggs"]


def test_limit_define_o_tamanho_do_composite():
    schema = eventos_schema()
    dataset = eventos_navegacao_es()
    request = QueryRequest(schema="eventos_navegacao", dimensions=("pais",), limit=20)
    columns = schema.columns_for(request)

    body = build_query_body(dataset, request, columns)

    assert body["aggs"]["grouped"]["composite"]["size"] == 20


def test_offset_e_ignorado():
    """Limitação documentada em `elasticsearch_dsl.py`: `composite` pagina via cursor
    `after_key`, que não existe entre chamadas `execute()` sem estado."""
    schema = eventos_schema()
    dataset = eventos_navegacao_es()
    columns = schema.columns_for(
        QueryRequest(schema="eventos_navegacao", dimensions=("pais",))
    )

    sem_offset = build_query_body(
        dataset, QueryRequest(schema="eventos_navegacao", dimensions=("pais",)), columns
    )
    com_offset = build_query_body(
        dataset,
        QueryRequest(schema="eventos_navegacao", dimensions=("pais",), offset=50),
        columns,
    )

    assert sem_offset == com_offset
    assert "after" not in str(com_offset)


def test_order_by_vira_bucket_sort():
    schema = eventos_schema()
    dataset = eventos_navegacao_es()
    request = QueryRequest(
        schema="eventos_navegacao",
        dimensions=("pais",),
        measures=("total_eventos",),
        order_by=(OrderBy(field="total_eventos", direction=SortDirection.DESC),),
    )
    columns = schema.columns_for(request)

    body = build_query_body(dataset, request, columns)

    assert body["aggs"]["grouped"]["aggs"]["_order"] == {
        "bucket_sort": {"sort": [{"total_eventos": {"order": "desc"}}]}
    }


# --- Filtros (seção 2.2, os 9 operadores) -----------------------------------------------


@pytest.mark.parametrize(
    "operator, value, expected_query",
    [
        (FilterOperator.EQ, "BR", {"bool": {"filter": [{"term": {"pais": "BR"}}]}}),
        (FilterOperator.NEQ, "BR", {"bool": {"must_not": [{"term": {"pais": "BR"}}]}}),
        (
            FilterOperator.IN,
            ["BR", "AR"],
            {"bool": {"filter": [{"terms": {"pais": ["BR", "AR"]}}]}},
        ),
        (
            FilterOperator.CONTAINS,
            "BR",
            {"bool": {"filter": [{"wildcard": {"pais": "*BR*"}}]}},
        ),
    ],
)
def test_operadores_de_filtro_sobre_dimensao_string(operator, value, expected_query):
    schema = eventos_schema()
    dataset = eventos_navegacao_es()
    request = QueryRequest(
        schema="eventos_navegacao",
        dimensions=("pais",),
        filters=(Filter(field="pais", operator=operator, value=value),),
    )
    columns = schema.columns_for(request)

    body = build_query_body(dataset, request, columns)

    assert body["query"] == expected_query


def _numeric_es_schema_and_dataset() -> tuple[Schema, Dataset]:
    """`eventos_navegacao_es` só tem dimensões `keyword` — sintético, local a este
    módulo, só para `between`/`gt`/`gte`/`lt`/`lte`, restritos a tipos não-string."""
    dataset = Dataset(
        name="pedidos_es",
        datasource=Datasource(type=DatasourceType.ELASTICSEARCH, connection_ref="env:TEST_ES_URL"),
        provides=Provides(dimensions={"ano"}, measures={"total"}),
        model=IndexModel(
            name="pedidos",
            mapping={
                "ano": FieldMapping(field="ano", es_type="integer"),
                "total": FieldMapping(field="valor", agg=Aggregation.SUM),
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
    "operator, value, expected_range",
    [
        (FilterOperator.BETWEEN, [2020, 2024], {"gte": 2020, "lte": 2024}),
        (FilterOperator.GT, 2020, {"gt": 2020}),
        (FilterOperator.GTE, 2020, {"gte": 2020}),
        (FilterOperator.LT, 2024, {"lt": 2024}),
        (FilterOperator.LTE, 2024, {"lte": 2024}),
    ],
)
def test_operadores_numericos_viram_range(operator, value, expected_range):
    schema, dataset = _numeric_es_schema_and_dataset()
    request = QueryRequest(
        schema="pedidos",
        dimensions=("ano",),
        filters=(Filter(field="ano", operator=operator, value=value),),
    )
    columns = schema.columns_for(request)

    body = build_query_body(dataset, request, columns)

    assert body["query"] == {"bool": {"filter": [{"range": {"ano": expected_range}}]}}


# --- parse_response ---------------------------------------------------------------------


def test_parse_response_com_dimensoes_le_os_buckets_do_composite():
    schema = eventos_schema()
    columns = schema.columns_for(
        QueryRequest(
            schema="eventos_navegacao",
            dimensions=("pais", "dispositivo"),
            measures=("duracao_media", "total_eventos"),
        )
    )
    response = {
        "took": 7,
        "aggregations": {
            "grouped": {
                "buckets": [
                    {
                        "key": {"pais": "BR", "dispositivo": "mobile"},
                        "duracao_media": {"value": 123.4},
                        "total_eventos": {"value": 58},
                    },
                    {
                        "key": {"pais": "AR", "dispositivo": "desktop"},
                        "duracao_media": {"value": 87.0},
                        "total_eventos": {"value": 12},
                    },
                ]
            }
        },
    }

    rows = parse_response(response, columns)

    assert rows == (
        ("BR", "mobile", 123.4, 58),
        ("AR", "desktop", 87.0, 12),
    )


def test_parse_response_sem_dimensoes_le_o_valor_do_topo():
    schema = eventos_schema()
    columns = schema.columns_for(
        QueryRequest(schema="eventos_navegacao", measures=("duracao_media",))
    )
    response = {"took": 3, "aggregations": {"duracao_media": {"value": 42.0}}}

    rows = parse_response(response, columns)

    assert rows == ((42.0,),)
