"""Parsing dos parâmetros planos do `GET /v1/query` (opção B da seção 2.2a).

```
?schema=vendas&dimensions=sigla_uf,cargo&measures=valor_total
&filter[sigla_uf][in]=SP,RJ&order_by=valor_total.desc&limit=100&offset=0
```

Módulo puro (sem FastAPI) para poder ser testado sem subir aplicação. O resultado é um
`QueryRequestModel` — o **mesmo** tipo produzido pelo corpo do POST e pela opção A
(`query=<json>`), de modo que as três formas convergem antes de virar `QueryRequest`.

Por que o `Schema` entra aqui: parâmetro de querystring é sempre texto, mas
`filter[ano][gt]=2020` sobre uma dimensão `number` precisa chegar no banco como número —
senão o driver compara inteiro com texto. A coerção usa o `type` declarado da dimensão
no catálogo; o JSON do POST não precisa disso porque já carrega tipos.
"""

import re
from collections.abc import Mapping
from datetime import date

from adapters.api.schemas import FilterModel, OrderByModel, QueryRequestModel
from domain.errors import InvalidFilterError, UnknownFieldError
from domain.models import DataType, FilterOperator, Schema, SortDirection

#: `filter[campo][operador]` — convenção JSON:API/Stripe citada na seção 2.2a.
_FILTER_KEY = re.compile(r"^filter\[([^\[\]]+)\]\[([^\[\]]+)\]$")

#: Operadores cujo valor é uma lista separada por vírgula (seção 2.2a).
_LIST_OPERATORS = {FilterOperator.IN, FilterOperator.BETWEEN}


def _split_list(raw: str) -> list[str]:
    return [part.strip() for part in raw.split(",") if part.strip()]


def _coerce(raw: str, data_type: DataType, field: str) -> object:
    """Texto da querystring → valor tipado, conforme o `type` da dimensão no catálogo."""
    if data_type is DataType.STRING:
        return raw
    try:
        if data_type is DataType.NUMBER:
            return int(raw) if re.fullmatch(r"[+-]?\d+", raw) else float(raw)
        if data_type is DataType.BOOLEAN:
            lowered = raw.lower()
            if lowered in ("true", "1"):
                return True
            if lowered in ("false", "0"):
                return False
            raise ValueError(raw)
        if data_type is DataType.DATE:
            return date.fromisoformat(raw)
    except ValueError as exc:
        raise InvalidFilterError(
            f"O valor '{raw}' não é válido para '{field}', do tipo `{data_type.value}`.",
            [field],
        ) from exc
    return raw


def _parse_operator(raw: str, field: str) -> FilterOperator:
    try:
        return FilterOperator(raw)
    except ValueError as exc:
        raise InvalidFilterError(
            f"Operador '{raw}' não existe (campo '{field}').", [field]
        ) from exc


def _parse_filters(params: Mapping[str, str], schema: Schema) -> list[FilterModel]:
    filters = []
    for key, raw_value in params.items():
        match = _FILTER_KEY.match(key)
        if match is None:
            continue
        field, raw_operator = match.group(1), match.group(2)
        operator = _parse_operator(raw_operator, field)

        dimension = schema.dimensions.get(field)
        if dimension is None:
            # Pode ser medida (rejeitada mais adiante por `validate_request`, com o erro
            # certo) ou campo inexistente — em ambos os casos, sem `type` para coagir.
            if field not in schema.measures:
                raise UnknownFieldError(
                    f"Campo inexistente no schema '{schema.name}': {field}.", [field]
                )
            value: object = raw_value
        elif operator in _LIST_OPERATORS:
            value = [_coerce(part, dimension.type, field) for part in _split_list(raw_value)]
        else:
            value = _coerce(raw_value, dimension.type, field)

        filters.append(FilterModel(field=field, operator=operator, value=value))
    return filters


def _parse_order_by(raw: str) -> list[OrderByModel]:
    """`valor_total.desc,sigla_uf.asc` — direção opcional, `asc` por omissão."""
    orders = []
    for item in _split_list(raw):
        field, _, raw_direction = item.rpartition(".")
        if not field:  # sem ponto: só o nome do campo
            field, raw_direction = item, SortDirection.ASC.value
        try:
            direction = SortDirection(raw_direction)
        except ValueError as exc:
            raise InvalidFilterError(
                f"Direção de ordenação inválida em '{item}' — use `asc` ou `desc`.",
                [field],
            ) from exc
        orders.append(OrderByModel(field=field, direction=direction))
    return orders


def _parse_int(params: Mapping[str, str], name: str) -> int | None:
    raw = params.get(name)
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except ValueError as exc:
        raise InvalidFilterError(f"`{name}` precisa ser um inteiro: '{raw}'.") from exc


def parse_flat_params(params: Mapping[str, str], schema: Schema) -> QueryRequestModel:
    """Monta o `QueryRequestModel` a partir dos parâmetros planos já lidos da URL."""
    offset = _parse_int(params, "offset")
    return QueryRequestModel(
        schema=schema.name,
        dimensions=_split_list(params.get("dimensions", "")),
        measures=_split_list(params.get("measures", "")),
        filters=_parse_filters(params, schema),
        order_by=_parse_order_by(params.get("order_by", "")),
        limit=_parse_int(params, "limit"),
        offset=offset if offset is not None else 0,
    )


def schema_name_from_params(params: Mapping[str, str]) -> str:
    """Nome do schema, necessário antes do resto para buscar o `Schema` no catálogo."""
    name = params.get("schema", "").strip()
    if not name:
        raise UnknownFieldError("A requisição precisa informar `schema`.")
    return name
