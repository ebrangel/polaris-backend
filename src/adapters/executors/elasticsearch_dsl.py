"""Traduz `Dataset` (`IndexModel`) + `QueryRequest` + `columns` em corpo de agregação
da Query DSL do Elasticsearch, e devolve linhas a partir da resposta — puro, sem I/O.

Elasticsearch não é relacional: não há `JOIN` nem `GROUP BY`; agregações fazem esse
papel (seção 1.1). Sempre usa `composite` quando há dimensões — uniformiza 1 e N
dimensões no mesmo corpo, e é a recomendação de `docs/escalabilidade.md` para
paginação eficiente de agregações.

Limitações documentadas aqui, de propósito (a própria seção 1.1 pede isso):

- **`offset` não é suportado.** A paginação real de `composite` usa o cursor
  `after_key` devolvido pela página anterior — não existe entre chamadas `execute()`
  sem estado. `request.offset` é ignorado (nunca lido abaixo).
- **`order_by` só considera o primeiro item.** Vira uma sub-agregação `bucket_sort`;
  ordenar por múltiplos critérios exigiria compor mais de um `bucket_sort`, fora de
  escopo deste marco.
- **`contains` vira `wildcard *valor*`** — aproximação; não há busca full-text real
  disponível sem um sub-campo `.text` mapeado, que o catálogo não declara.
"""

from typing import Any

from domain.models import Aggregation
from domain.models import Column as DomainColumn
from domain.models import Dataset, FieldMapping, QueryRequest, SortDirection

_METRIC_AGG = {
    Aggregation.SUM: "sum",
    Aggregation.AVG: "avg",
    Aggregation.VALUE_COUNT: "value_count",
    Aggregation.COUNT: "value_count",
    Aggregation.MIN: "min",
    Aggregation.MAX: "max",
    Aggregation.COUNT_DISTINCT: "cardinality",
}


def _physical_field(dataset: Dataset, logical_name: str) -> FieldMapping:
    mapping = dataset.physical_for(logical_name)
    assert isinstance(mapping, FieldMapping)
    return mapping


def _filter_clauses(dataset: Dataset, request: QueryRequest) -> tuple[list[dict], list[dict]]:
    """(cláusulas `filter`, cláusulas `must_not`) do `bool` query."""
    filter_clauses: list[dict] = []
    must_not_clauses: list[dict] = []
    for f in request.filters:
        field = _physical_field(dataset, f.field).field
        value: Any = f.value
        match f.operator.value:
            case "eq":
                filter_clauses.append({"term": {field: value}})
            case "neq":
                must_not_clauses.append({"term": {field: value}})
            case "in":
                filter_clauses.append({"terms": {field: list(value)}})
            case "between":
                filter_clauses.append({"range": {field: {"gte": value[0], "lte": value[1]}}})
            case "gt":
                filter_clauses.append({"range": {field: {"gt": value}}})
            case "gte":
                filter_clauses.append({"range": {field: {"gte": value}}})
            case "lt":
                filter_clauses.append({"range": {field: {"lt": value}}})
            case "lte":
                filter_clauses.append({"range": {field: {"lte": value}}})
            case "contains":
                filter_clauses.append({"wildcard": {field: f"*{value}*"}})
    return filter_clauses, must_not_clauses


def _metric_aggs(dataset: Dataset, measure_fields: list[str]) -> dict[str, dict]:
    aggs = {}
    for name in measure_fields:
        mapping = _physical_field(dataset, name)
        aggs[name] = {_METRIC_AGG[mapping.agg]: {"field": mapping.field}}
    return aggs


def _composite_sources(dataset: Dataset, dimension_fields: list[str]) -> list[dict]:
    return [
        {name: {"terms": {"field": _physical_field(dataset, name).field}}}
        for name in dimension_fields
    ]


def _bucket_sort(request: QueryRequest) -> dict | None:
    if not request.order_by:
        return None
    order = request.order_by[0]
    direction = "desc" if order.direction is SortDirection.DESC else "asc"
    return {"bucket_sort": {"sort": [{order.field: {"order": direction}}]}}


def build_query_body(
    dataset: Dataset, request: QueryRequest, columns: tuple[DomainColumn, ...]
) -> dict:
    """Corpo de `_search`: sempre `size: 0` (só agregações, sem hits de documento)."""
    field_names = [c.field for c in columns]
    measure_fields = [name for name in field_names if name in dataset.provides.measures]
    dimension_fields = [name for name in field_names if name not in dataset.provides.measures]

    filter_clauses, must_not_clauses = _filter_clauses(dataset, request)
    query: dict = {"match_all": {}}
    if filter_clauses or must_not_clauses:
        bool_query: dict = {}
        if filter_clauses:
            bool_query["filter"] = filter_clauses
        if must_not_clauses:
            bool_query["must_not"] = must_not_clauses
        query = {"bool": bool_query}

    body: dict = {"size": 0, "query": query}
    metric_aggs = _metric_aggs(dataset, measure_fields)

    if not dimension_fields:
        body["aggs"] = metric_aggs
        return body

    composite_agg: dict = {
        "composite": {
            "size": request.limit if request.limit is not None else 1000,
            "sources": _composite_sources(dataset, dimension_fields),
        }
    }
    sub_aggs = dict(metric_aggs)
    bucket_sort = _bucket_sort(request)
    if bucket_sort is not None:
        sub_aggs["_order"] = bucket_sort
    if sub_aggs:
        composite_agg["aggs"] = sub_aggs

    body["aggs"] = {"grouped": composite_agg}
    return body


def parse_response(response: dict, columns: tuple[DomainColumn, ...]) -> tuple[tuple, ...]:
    """Extrai as linhas da resposta do Elasticsearch, na ordem de `columns`."""
    field_names = [c.field for c in columns]
    aggregations = response.get("aggregations", {})

    if "grouped" not in aggregations:
        # Sem dimensão pedida: uma única linha, métricas direto no topo da agregação.
        return (tuple(aggregations[name]["value"] for name in field_names),)

    rows = []
    for bucket in aggregations["grouped"]["buckets"]:
        row = tuple(
            bucket["key"][name] if name in bucket["key"] else bucket[name]["value"]
            for name in field_names
        )
        rows.append(row)
    return tuple(rows)
