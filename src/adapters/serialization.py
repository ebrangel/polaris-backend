"""Serialização JSON-safe compartilhada entre o cache (Redis) e a fila (`arq`).

`QueryRequest`/`QueryResult` (domain) ↔ `dict` — usada tanto para gravar no Redis
(`RedisCacheGateway`) quanto para passar como argumento/resultado de job
(`ArqJobQueue`/`tasks.py`). O `arq` serializa argumentos e resultado de job com
`pickle` por padrão; usar apenas dicionários evita depender de como o pickle trata as
dataclasses de domínio entre versões de Python, e o worker não precisa importar as
classes de domínio para desserializar — só este módulo.

`jsonable()` é o mesmo tratamento de `Decimal`/`date` escrito no Marco 6 para as
respostas HTTP (`adapters/api/schemas.py`), extraído para cá porque cache e fila
passam pelo mesmo problema: valores vindos direto do driver do banco.
"""

from datetime import date, datetime, time
from decimal import Decimal
from typing import Any
from uuid import UUID

from domain.models import (
    Column,
    DataType,
    Filter,
    FilterOperator,
    OrderBy,
    QueryRequest,
    QueryResult,
    QueryStatus,
    SortDirection,
)


def jsonable(value: Any) -> Any:
    """Valor vindo do driver (ou já em memória) → tipo serializável em JSON.

    `Decimal` vira `str`, não `float`, para não perder precisão. Listas e tuplas viram
    listas recursivamente (valores de filtro `in`/`between` são tuplas).
    """
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, (list, tuple)):
        return [jsonable(item) for item in value]
    return value


def request_to_dict(request: QueryRequest) -> dict[str, Any]:
    return {
        "schema": request.schema,
        "dimensions": list(request.dimensions),
        "measures": list(request.measures),
        "filters": [
            {"field": f.field, "operator": f.operator.value, "value": jsonable(f.value)}
            for f in request.filters
        ],
        "order_by": [
            {"field": o.field, "direction": o.direction.value} for o in request.order_by
        ],
        "limit": request.limit,
        "offset": request.offset,
    }


def dict_to_request(data: dict[str, Any]) -> QueryRequest:
    """Reconstrói o `QueryRequest`.

    Valores de filtro chegam com o mesmo tipo nativo de JSON que teriam vindo direto de
    `POST /v1/query` (`str`/`int`/`float`/`bool`/lista) — mesma limitação de tipo que já
    existe na borda HTTP; não há metadado extra aqui para reconstruir, por exemplo, um
    `date` original a partir da string ISO.
    """
    return QueryRequest(
        schema=data["schema"],
        dimensions=tuple(data.get("dimensions", ())),
        measures=tuple(data.get("measures", ())),
        filters=tuple(
            Filter(field=f["field"], operator=FilterOperator(f["operator"]), value=f["value"])
            for f in data.get("filters", ())
        ),
        order_by=tuple(
            OrderBy(field=o["field"], direction=SortDirection(o["direction"]))
            for o in data.get("order_by", ())
        ),
        limit=data.get("limit"),
        offset=data.get("offset", 0),
    )


def result_to_dict(result: QueryResult) -> dict[str, Any]:
    """Ao contrário do presenter HTTP (`present_result`), sempre inclui `format` (mesmo
    `None`): aqui o objetivo é fidelidade de round-trip, não a forma enxuta da seção 2.3.
    """
    body: dict[str, Any] = {"query_id": result.query_id, "status": result.status.value}
    if result.status is QueryStatus.PROCESSING:
        return body
    if result.status is QueryStatus.FAILED:
        body["error"] = result.error
        return body

    assert result.meta is not None
    body["columns"] = [
        {"field": c.field, "type": c.type.value, "format": c.format} for c in result.columns
    ]
    # `rows=None` (resultado transmitido, `QueryResult.streamed`) vira `None` no dicionário
    # e não uma lista vazia: é a diferença entre "as linhas estão no artefato" e "o
    # resultado tem zero linhas", e o round-trip precisa preservá-la. É o que deixa o
    # valor de retorno do job no `arq` ser só o descritor, sem carregar as linhas.
    body["rows"] = (
        None
        if result.rows is None
        else [[jsonable(value) for value in row] for row in result.rows]
    )
    body["meta"] = {
        "row_count": result.meta.row_count,
        "cached": result.meta.cached,
        "execution_ms": result.meta.execution_ms,
        "dataset_used": result.meta.dataset_used,
        "total_rows": result.meta.total_rows,
    }
    return body


def dict_to_result(data: dict[str, Any]) -> QueryResult:
    status = QueryStatus(data["status"])
    if status is QueryStatus.PROCESSING:
        return QueryResult.processing(data["query_id"])
    if status is QueryStatus.FAILED:
        return QueryResult.failed(data["query_id"], error=data["error"])

    columns = tuple(
        Column(field=c["field"], type=DataType(c["type"]), format=c.get("format"))
        for c in data["columns"]
    )
    meta = data["meta"]
    rows = data.get("rows")
    if rows is None:
        return QueryResult.streamed(
            query_id=data["query_id"],
            columns=columns,
            row_count=meta["row_count"],
            total_rows=meta.get("total_rows"),
            dataset_used=meta["dataset_used"],
            cached=meta["cached"],
            execution_ms=meta["execution_ms"],
        )
    return QueryResult.completed(
        query_id=data["query_id"],
        columns=columns,
        rows=[tuple(row) for row in rows],
        dataset_used=meta["dataset_used"],
        cached=meta["cached"],
        execution_ms=meta["execution_ms"],
        total_rows=meta.get("total_rows"),
    )
