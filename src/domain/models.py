"""Entidades de domínio — Python puro, sem framework algum.

Duas famílias de entidades:

* **Catálogo** — `Schema` (modelo lógico: `Dimension`/`Measure`) e os `Dataset` que o
  atendem, cada um com seu `Datasource` e seu modelo físico (`TableModel`, `StarModel`
  ou `IndexModel`).
* **Consulta** — `QueryRequest` (ponto de convergência de `POST` e `GET /v1/query`) e
  `QueryResult`.

As invariantes são verificadas na construção: um `Schema` que exista em memória é,
por construção, um schema válido.

Referência: `docs/catalogo-e-contrato-completo.md`.
"""

import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from types import MappingProxyType
from typing import Any

from domain.errors import (
    ForbiddenMeasureError,
    InvalidCatalogError,
    InvalidFilterError,
    UnknownFieldError,
    UnknownSchemaError,
)

__all__ = [
    "AccessControl",
    "Aggregation",
    "Catalog",
    "CatalogVersion",
    "Column",
    "ColumnMapping",
    "DataType",
    "Dataset",
    "Datasource",
    "DatasourceType",
    "Dimension",
    "DimensionTable",
    "Fact",
    "FactKey",
    "FieldMapping",
    "Filter",
    "FilterOperator",
    "IndexModel",
    "Join",
    "Measure",
    "OrderBy",
    "PhysicalModel",
    "Provides",
    "QueryRequest",
    "QueryResult",
    "QueryStatus",
    "ResultMeta",
    "Schema",
    "SortDirection",
    "StarModel",
    "TableModel",
]


# --------------------------------------------------------------------------------------
# Enums
# --------------------------------------------------------------------------------------


class DataType(str, Enum):
    """Tipo de um campo. `string`/`number` são os tipos da resposta (seção 2.3)."""

    STRING = "string"
    NUMBER = "number"
    BOOLEAN = "boolean"
    DATE = "date"


class Aggregation(str, Enum):
    """Função de agregação de uma medida.

    `sum`, `avg` e `value_count` aparecem no catálogo documentado; as demais são
    extensão para os casos previsíveis de SQL.
    """

    SUM = "sum"
    AVG = "avg"
    VALUE_COUNT = "value_count"
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"
    MIN = "min"
    MAX = "max"


class FilterOperator(str, Enum):
    """Operadores de filtro suportados (seção 2.2) — conjunto fechado."""

    EQ = "eq"
    NEQ = "neq"
    IN = "in"
    BETWEEN = "between"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    CONTAINS = "contains"


class SortDirection(str, Enum):
    ASC = "asc"
    DESC = "desc"


class QueryStatus(str, Enum):
    """Estados de uma consulta (seções 2.3 e 2.4)."""

    COMPLETED = "completed"
    PROCESSING = "processing"
    FAILED = "failed"


class DatasourceType(str, Enum):
    """Engine do datasource."""

    POSTGRES = "postgres"
    ORACLE = "oracle"
    ELASTICSEARCH = "elasticsearch"


#: Operadores válidos por tipo de campo. `contains` só vale para `string` (seção 2.2).
_OPERATORS_BY_TYPE: Mapping[DataType, frozenset[FilterOperator]] = MappingProxyType(
    {
        DataType.STRING: frozenset(
            {
                FilterOperator.EQ,
                FilterOperator.NEQ,
                FilterOperator.IN,
                FilterOperator.CONTAINS,
            }
        ),
        DataType.NUMBER: frozenset(
            {
                FilterOperator.EQ,
                FilterOperator.NEQ,
                FilterOperator.IN,
                FilterOperator.BETWEEN,
                FilterOperator.GT,
                FilterOperator.GTE,
                FilterOperator.LT,
                FilterOperator.LTE,
            }
        ),
        DataType.DATE: frozenset(
            {
                FilterOperator.EQ,
                FilterOperator.NEQ,
                FilterOperator.IN,
                FilterOperator.BETWEEN,
                FilterOperator.GT,
                FilterOperator.GTE,
                FilterOperator.LT,
                FilterOperator.LTE,
            }
        ),
        DataType.BOOLEAN: frozenset({FilterOperator.EQ, FilterOperator.NEQ}),
    }
)


def _freeze(mapping: Mapping[str, Any]) -> Mapping[str, Any]:
    """Congela um mapping para que a entidade permaneça imutável."""
    return MappingProxyType(dict(mapping))


# --------------------------------------------------------------------------------------
# Modelo lógico
# --------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Dimension:
    """Dimensão canônica do modelo lógico (ex: `sigla_uf`)."""

    name: str
    type: DataType = DataType.STRING
    filterable: bool = True

    def supports(self, operator: FilterOperator) -> bool:
        """Indica se o operador é válido para o tipo desta dimensão."""
        return operator in _OPERATORS_BY_TYPE[self.type]


@dataclass(frozen=True, slots=True)
class Measure:
    """Medida canônica do modelo lógico (ex: `valor_total`)."""

    name: str
    agg: Aggregation
    format: str | None = None


@dataclass(frozen=True, slots=True)
class AccessControl:
    """Controle de acesso do schema: role → medidas visíveis (seção 1.0)."""

    roles: Mapping[str, frozenset[str]]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "roles",
            _freeze({role: frozenset(measures) for role, measures in self.roles.items()}),
        )

    def allowed_measures(self, roles: Iterable[str]) -> frozenset[str]:
        """União das medidas permitidas para os roles informados."""
        allowed: set[str] = set()
        for role in roles:
            allowed |= self.roles.get(role, frozenset())
        return frozenset(allowed)


# --------------------------------------------------------------------------------------
# Modelo físico
# --------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ColumnMapping:
    """Mapeamento de um campo lógico para uma coluna de banco relacional."""

    column: str
    agg: Aggregation | None = None


@dataclass(frozen=True, slots=True)
class FieldMapping:
    """Mapeamento de um campo lógico para um campo de índice Elasticsearch."""

    field: str
    es_type: str | None = None
    agg: Aggregation | None = None


@dataclass(frozen=True, slots=True)
class TableModel:
    """Tabela/visão única — modelo plano ou agregado (seções 1.0 e 1.2)."""

    source: str
    mapping: Mapping[str, ColumnMapping]

    def __post_init__(self) -> None:
        object.__setattr__(self, "mapping", _freeze(self.mapping))

    def mapped_fields(self) -> frozenset[str]:
        return frozenset(self.mapping)


@dataclass(frozen=True, slots=True)
class FactKey:
    """Chave estrangeira do fato para uma tabela de dimensão."""

    column: str
    references: str  # "<alias_da_dimensao>.<chave>"

    @property
    def dimension_alias(self) -> str:
        return self.references.split(".", 1)[0]


@dataclass(frozen=True, slots=True)
class Fact:
    """Tabela fato de um dataset em star schema."""

    table: str
    mapping: Mapping[str, ColumnMapping]
    keys: Mapping[str, FactKey] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "mapping", _freeze(self.mapping))
        object.__setattr__(self, "keys", _freeze(self.keys))


@dataclass(frozen=True, slots=True)
class DimensionTable:
    """Tabela de dimensão de um dataset em star schema."""

    table: str
    primary_key: str
    mapping: Mapping[str, ColumnMapping]

    def __post_init__(self) -> None:
        object.__setattr__(self, "mapping", _freeze(self.mapping))


@dataclass(frozen=True, slots=True)
class Join:
    """Junção entre fato e dimensão, no formato `<alias>.<chave>` dos dois lados."""

    from_ref: str
    to_ref: str

    @property
    def to_alias(self) -> str:
        return self.to_ref.split(".", 1)[0]


@dataclass(frozen=True, slots=True)
class StarModel:
    """Fato + dimensões + joins, todos no mesmo datasource (seção 1.0)."""

    fact: Fact
    dimension_tables: Mapping[str, DimensionTable]
    joins: tuple[Join, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "dimension_tables", _freeze(self.dimension_tables))
        object.__setattr__(self, "joins", tuple(self.joins))

        # Joins e chaves do fato só podem apontar para dimensões declaradas no dataset.
        # Apenas o alias da tabela é verificado: a documentação usa `references:
        # dim_cliente.id` enquanto a `primary_key` é `CD_CLIENTE`, então o nome da chave
        # depois do ponto não é uma referência confiável.
        for key_name, key in self.fact.keys.items():
            if key.dimension_alias not in self.dimension_tables:
                raise InvalidCatalogError(
                    f"A chave '{key_name}' do fato referencia a dimensão "
                    f"'{key.dimension_alias}', que não está declarada no dataset.",
                    [key_name],
                )
        for join in self.joins:
            if join.to_alias not in self.dimension_tables:
                raise InvalidCatalogError(
                    f"O join para '{join.to_ref}' referencia a dimensão "
                    f"'{join.to_alias}', que não está declarada no dataset.",
                    [join.to_ref],
                )

    def mapped_fields(self) -> frozenset[str]:
        fields = set(self.fact.mapping)
        for dimension_table in self.dimension_tables.values():
            fields |= set(dimension_table.mapping)
        return frozenset(fields)


@dataclass(frozen=True, slots=True)
class IndexModel:
    """Índice único e denormalizado do Elasticsearch (seção 1.1)."""

    name: str
    mapping: Mapping[str, FieldMapping]

    def __post_init__(self) -> None:
        object.__setattr__(self, "mapping", _freeze(self.mapping))

    def mapped_fields(self) -> frozenset[str]:
        return frozenset(self.mapping)


PhysicalModel = TableModel | StarModel | IndexModel


# --------------------------------------------------------------------------------------
# Dataset
# --------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Datasource:
    """Engine + referência de conexão de um dataset."""

    type: DatasourceType
    connection_ref: str


@dataclass(frozen=True, slots=True)
class Provides:
    """Campos do modelo lógico que um dataset consegue atender."""

    dimensions: frozenset[str] = frozenset()
    measures: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        object.__setattr__(self, "dimensions", frozenset(self.dimensions))
        object.__setattr__(self, "measures", frozenset(self.measures))

    def fields(self) -> frozenset[str]:
        return self.dimensions | self.measures


@dataclass(frozen=True, slots=True)
class Dataset:
    """Fonte física que atende parte (ou todo) o modelo lógico de um schema."""

    name: str
    datasource: Datasource
    provides: Provides
    model: PhysicalModel

    def __post_init__(self) -> None:
        is_elasticsearch = self.datasource.type is DatasourceType.ELASTICSEARCH
        is_index = isinstance(self.model, IndexModel)
        if is_elasticsearch and not is_index:
            raise InvalidCatalogError(
                f"O dataset '{self.name}' é elasticsearch e só suporta modelo plano "
                f"(bloco `index`), não {type(self.model).__name__}.",
                [self.name],
            )
        if is_index and not is_elasticsearch:
            raise InvalidCatalogError(
                f"O dataset '{self.name}' declara um índice, mas seu datasource é "
                f"'{self.datasource.type.value}'.",
                [self.name],
            )

        missing = sorted(self.provides.fields() - self.model.mapped_fields())
        if missing:
            raise InvalidCatalogError(
                f"O dataset '{self.name}' declara em `provides` campos sem mapeamento "
                f"físico: {', '.join(missing)}.",
                missing,
            )

    def covers(self, dimensions: Iterable[str], measures: Iterable[str]) -> bool:
        """Indica se este dataset cobre todas as dimensões e medidas pedidas.

        É a condição do `select_dataset` da seção 1.0. A iteração em ordem de
        declaração é do use case `ResolveDataset` (Marco 3) — aqui só o predicado.
        """
        return (
            frozenset(dimensions) <= self.provides.dimensions
            and frozenset(measures) <= self.provides.measures
        )

    def physical_for(self, logical_name: str) -> ColumnMapping | FieldMapping:
        """Traduz um nome lógico para seu mapeamento físico neste dataset."""
        if isinstance(self.model, StarModel):
            if logical_name in self.model.fact.mapping:
                return self.model.fact.mapping[logical_name]
            for dimension_table in self.model.dimension_tables.values():
                if logical_name in dimension_table.mapping:
                    return dimension_table.mapping[logical_name]
        elif logical_name in self.model.mapping:
            return self.model.mapping[logical_name]
        raise UnknownFieldError(
            f"O campo '{logical_name}' não está mapeado no dataset '{self.name}'.",
            [logical_name],
        )


# --------------------------------------------------------------------------------------
# Consulta — requisição
# --------------------------------------------------------------------------------------


def _is_sequence(value: Any) -> bool:
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes))


@dataclass(frozen=True, slots=True)
class Filter:
    """Filtro sobre um campo do modelo lógico (seção 2.2)."""

    field: str
    operator: FilterOperator
    value: Any

    def __post_init__(self) -> None:
        if self.operator is FilterOperator.IN:
            if not _is_sequence(self.value) or len(self.value) == 0:
                raise InvalidFilterError(
                    f"O operador `in` sobre '{self.field}' exige uma lista não vazia.",
                    [self.field],
                )
            object.__setattr__(self, "value", tuple(self.value))
        elif self.operator is FilterOperator.BETWEEN:
            if not _is_sequence(self.value) or len(self.value) != 2:
                raise InvalidFilterError(
                    f"O operador `between` sobre '{self.field}' exige exatamente dois "
                    f"valores.",
                    [self.field],
                )
            object.__setattr__(self, "value", tuple(self.value))
        elif _is_sequence(self.value):
            raise InvalidFilterError(
                f"O operador `{self.operator.value}` sobre '{self.field}' exige um "
                f"valor escalar.",
                [self.field],
            )


@dataclass(frozen=True, slots=True)
class OrderBy:
    """Ordenação por um campo do modelo lógico (seção 2.2)."""

    field: str
    direction: SortDirection = SortDirection.ASC


@dataclass(frozen=True, slots=True)
class QueryRequest:
    """Consulta estruturada, em nomes lógicos do schema.

    É o objeto para onde `POST /v1/query` e `GET /v1/query` convergem antes de chegar
    ao use case — nenhuma das duas rotas carrega lógica de negócio própria.
    """

    schema: str
    dimensions: tuple[str, ...] = ()
    measures: tuple[str, ...] = ()
    filters: tuple[Filter, ...] = ()
    order_by: tuple[OrderBy, ...] = ()
    limit: int | None = None
    offset: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "dimensions", tuple(self.dimensions))
        object.__setattr__(self, "measures", tuple(self.measures))
        object.__setattr__(self, "filters", tuple(self.filters))
        object.__setattr__(self, "order_by", tuple(self.order_by))

        if not self.schema:
            raise UnknownSchemaError("A requisição precisa informar um schema.")
        if not self.dimensions and not self.measures:
            raise UnknownFieldError(
                "A requisição precisa pedir ao menos uma dimensão ou medida."
            )
        if self.limit is not None and self.limit < 0:
            raise InvalidFilterError(f"`limit` não pode ser negativo: {self.limit}.")
        if self.offset < 0:
            raise InvalidFilterError(f"`offset` não pode ser negativo: {self.offset}.")

    def filter_fields(self) -> frozenset[str]:
        """Campos usados em filtros — contam para a cobertura de dataset (seção 1.0)."""
        return frozenset(f.field for f in self.filters)

    def order_fields(self) -> frozenset[str]:
        """Campos usados em ordenação — também contam para a cobertura."""
        return frozenset(o.field for o in self.order_by)

    def referenced_fields(self) -> frozenset[str]:
        """Todos os campos citados pela requisição, de saída ou não."""
        return (
            frozenset(self.dimensions)
            | frozenset(self.measures)
            | self.filter_fields()
            | self.order_fields()
        )

    def referenced_fields_in_order(self) -> tuple[str, ...]:
        """`referenced_fields()`, mas na ordem em que o cliente pediu, sem repetição.

        Usada para montar mensagens de erro como a da seção 2.5 ("...combinação de
        campos: sigla_uf, cargo, canal"), que segue a ordem do pedido, não alfabética.
        Não itera sobre os `frozenset` de `filter_fields()`/`order_fields()` — a ordem
        de iteração de um set de strings não é garantida entre execuções — e sim sobre
        `filters`/`order_by`, que preservam a ordem de declaração.
        """
        candidates = (
            list(self.dimensions)
            + list(self.measures)
            + [f.field for f in self.filters]
            + [o.field for o in self.order_by]
        )
        seen: set[str] = set()
        result = []
        for field in candidates:
            if field not in seen:
                seen.add(field)
                result.append(field)
        return tuple(result)

    def _canonical(self) -> str:
        """Serialização canônica usada como identidade da requisição.

        A ordem de `dimensions`, `measures` e `order_by` é preservada porque muda o
        resultado (ordem das colunas e das linhas). Os filtros são ordenados porque
        formam uma conjunção: trocá-los de lugar não muda o resultado.
        """

        def filter_key(f: Filter) -> str:
            value = f.value
            if f.operator is FilterOperator.IN:
                value = sorted(value, key=lambda v: json.dumps(v, default=str))
            return json.dumps([f.field, f.operator.value, value], default=str, sort_keys=True)

        payload = {
            "schema": self.schema,
            "dimensions": list(self.dimensions),
            "measures": list(self.measures),
            "filters": sorted(filter_key(f) for f in self.filters),
            "order_by": [[o.field, o.direction.value] for o in self.order_by],
            "limit": self.limit,
            "offset": self.offset,
        }
        return json.dumps(payload, default=str, sort_keys=True, separators=(",", ":"))

    def fingerprint(self) -> str:
        """SHA-256 da requisição canônica — base do `query_id` e da chave de cache."""
        return hashlib.sha256(self._canonical().encode("utf-8")).hexdigest()

    @property
    def query_id(self) -> str:
        """Identificador da consulta, no formato `q_8f2a1c` das seções 2.3 e 2.4."""
        return f"q_{self.fingerprint()[:6]}"


# --------------------------------------------------------------------------------------
# Consulta — resultado
# --------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Column:
    """Coluna da resposta (seção 2.3)."""

    field: str
    type: DataType
    format: str | None = None


@dataclass(frozen=True, slots=True)
class ResultMeta:
    """Metadados de execução da consulta (seção 2.3)."""

    row_count: int
    cached: bool
    execution_ms: int
    dataset_used: str


@dataclass(frozen=True, slots=True)
class QueryResult:
    """Resultado de uma consulta, síncrono (seção 2.3) ou assíncrono (seção 2.4).

    `poll_url` não faz parte do domínio: é detalhe de transporte, montado a partir do
    `query_id` pelo adapter de API.
    """

    query_id: str
    status: QueryStatus
    columns: tuple[Column, ...] = ()
    rows: tuple[tuple[Any, ...], ...] = ()
    meta: ResultMeta | None = None
    error: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "columns", tuple(self.columns))
        object.__setattr__(self, "rows", tuple(tuple(row) for row in self.rows))

        # Violar estas invariantes é erro de programação de quem monta o resultado
        # (executor ou use case), não erro do cliente nem do catálogo.
        if self.status is QueryStatus.COMPLETED:
            if self.meta is None:
                raise ValueError(
                    f"O resultado '{self.query_id}' está concluído mas não tem `meta`."
                )
            width = len(self.columns)
            for index, row in enumerate(self.rows):
                if len(row) != width:
                    raise ValueError(
                        f"A linha {index} tem {len(row)} valores, mas o resultado "
                        f"declara {width} colunas."
                    )
            if self.meta.row_count != len(self.rows):
                raise ValueError(
                    f"`meta.row_count` ({self.meta.row_count}) não corresponde ao "
                    f"número de linhas ({len(self.rows)})."
                )
        else:
            if self.rows or self.columns or self.meta is not None:
                raise ValueError(
                    f"Um resultado com status '{self.status.value}' não carrega "
                    f"colunas, linhas nem `meta`."
                )
            if self.status is QueryStatus.FAILED and not self.error:
                raise ValueError(
                    f"O resultado '{self.query_id}' falhou mas não informa o erro."
                )

    @classmethod
    def completed(
        cls,
        query_id: str,
        columns: Iterable[Column],
        rows: Iterable[Sequence[Any]],
        *,
        dataset_used: str,
        cached: bool = False,
        execution_ms: int = 0,
    ) -> "QueryResult":
        """Resultado pronto — `meta.row_count` é derivado das linhas."""
        materialized = tuple(tuple(row) for row in rows)
        return cls(
            query_id=query_id,
            status=QueryStatus.COMPLETED,
            columns=tuple(columns),
            rows=materialized,
            meta=ResultMeta(
                row_count=len(materialized),
                cached=cached,
                execution_ms=execution_ms,
                dataset_used=dataset_used,
            ),
        )

    @classmethod
    def processing(cls, query_id: str) -> "QueryResult":
        """Consulta enfileirada, ainda sem resultado (seção 2.4)."""
        return cls(query_id=query_id, status=QueryStatus.PROCESSING)

    @classmethod
    def failed(cls, query_id: str, error: str) -> "QueryResult":
        return cls(query_id=query_id, status=QueryStatus.FAILED, error=error)


# --------------------------------------------------------------------------------------
# Schema
# --------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Schema:
    """Modelo lógico de um assunto, com os datasets que podem atendê-lo.

    A ordem de `datasets` é a política de otimização: o resolvedor usa o primeiro que
    cobre a requisição (seção 1.0).
    """

    name: str
    version: int
    dimensions: Mapping[str, Dimension]
    measures: Mapping[str, Measure]
    datasets: tuple[Dataset, ...] = ()
    description: str | None = None
    access_control: AccessControl | None = None
    max_limit: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "dimensions", _freeze(self.dimensions))
        object.__setattr__(self, "measures", _freeze(self.measures))
        object.__setattr__(self, "datasets", tuple(self.datasets))

        for name, dimension in self.dimensions.items():
            if name != dimension.name:
                raise InvalidCatalogError(
                    f"A dimensão está indexada como '{name}' mas se chama "
                    f"'{dimension.name}'.",
                    [name],
                )
        for name, measure in self.measures.items():
            if name != measure.name:
                raise InvalidCatalogError(
                    f"A medida está indexada como '{name}' mas se chama "
                    f"'{measure.name}'.",
                    [name],
                )

        overlapping = sorted(set(self.dimensions) & set(self.measures))
        if overlapping:
            raise InvalidCatalogError(
                f"Os nomes {', '.join(overlapping)} são ao mesmo tempo dimensão e "
                f"medida do schema '{self.name}'.",
                overlapping,
            )

        seen: set[str] = set()
        for dataset in self.datasets:
            if dataset.name in seen:
                raise InvalidCatalogError(
                    f"O schema '{self.name}' tem mais de um dataset chamado "
                    f"'{dataset.name}'.",
                    [dataset.name],
                )
            seen.add(dataset.name)

            unknown_dimensions = sorted(dataset.provides.dimensions - set(self.dimensions))
            unknown_measures = sorted(dataset.provides.measures - set(self.measures))
            if unknown_dimensions or unknown_measures:
                unknown = unknown_dimensions + unknown_measures
                raise InvalidCatalogError(
                    f"O dataset '{dataset.name}' declara em `provides` campos que não "
                    f"existem no modelo lógico de '{self.name}': {', '.join(unknown)}.",
                    unknown,
                )

        if self.access_control is not None:
            for role, measures in self.access_control.roles.items():
                unknown = sorted(measures - set(self.measures))
                if unknown:
                    raise InvalidCatalogError(
                        f"O role '{role}' referencia medidas inexistentes no schema "
                        f"'{self.name}': {', '.join(unknown)}.",
                        unknown,
                    )

    def split_fields(self, names: Iterable[str]) -> tuple[frozenset[str], frozenset[str]]:
        """Separa nomes lógicos em (dimensões, medidas), levantando erro nos desconhecidos.

        É o que o Marco 3 usa para montar a cobertura exigida: o pseudocódigo da seção
        1.0 joga os campos de ordenação em `required_dims`, mas o exemplo da seção 2.2
        ordena por `valor_total`, que é medida.
        """
        dimensions: set[str] = set()
        measures: set[str] = set()
        unknown: list[str] = []
        for name in names:
            if name in self.dimensions:
                dimensions.add(name)
            elif name in self.measures:
                measures.add(name)
            else:
                unknown.append(name)
        if unknown:
            raise UnknownFieldError(
                f"Campos inexistentes no schema '{self.name}': {', '.join(sorted(unknown))}.",
                sorted(unknown),
            )
        return frozenset(dimensions), frozenset(measures)

    def validate_request(self, request: QueryRequest) -> None:
        """Passo (1) do fluxo da seção 3: valida a requisição contra o modelo lógico."""
        if request.schema != self.name:
            raise UnknownSchemaError(
                f"A requisição é para o schema '{request.schema}', mas foi validada "
                f"contra '{self.name}'.",
                [request.schema],
            )

        unknown = sorted(request.referenced_fields() - set(self.dimensions) - set(self.measures))
        if unknown:
            raise UnknownFieldError(
                f"Campos inexistentes no schema '{self.name}': {', '.join(unknown)}.",
                unknown,
            )

        wrong_kind = sorted(set(request.dimensions) & set(self.measures))
        if wrong_kind:
            raise UnknownFieldError(
                f"Os campos {', '.join(wrong_kind)} são medidas, não dimensões.",
                wrong_kind,
            )
        wrong_kind = sorted(set(request.measures) & set(self.dimensions))
        if wrong_kind:
            raise UnknownFieldError(
                f"Os campos {', '.join(wrong_kind)} são dimensões, não medidas.",
                wrong_kind,
            )

        for filter_ in request.filters:
            dimension = self.dimensions.get(filter_.field)
            if dimension is None:
                raise InvalidFilterError(
                    f"Só é possível filtrar por dimensões; '{filter_.field}' é medida.",
                    [filter_.field],
                )
            if not dimension.filterable:
                raise InvalidFilterError(
                    f"A dimensão '{filter_.field}' não é filtrável.",
                    [filter_.field],
                )
            if not dimension.supports(filter_.operator):
                raise InvalidFilterError(
                    f"O operador `{filter_.operator.value}` não é válido para "
                    f"'{filter_.field}', do tipo `{dimension.type.value}`.",
                    [filter_.field],
                )

    def authorize(self, request: QueryRequest, roles: Iterable[str]) -> None:
        """Verifica se os roles do usuário dão acesso às medidas pedidas (seção 1.0)."""
        if self.access_control is None:
            return
        allowed = self.access_control.allowed_measures(roles)
        forbidden = sorted(set(request.measures) - allowed)
        if forbidden:
            raise ForbiddenMeasureError(
                f"As medidas {', '.join(forbidden)} não estão liberadas para os roles "
                f"informados.",
                forbidden,
            )

    def effective_limit(self, requested: int | None) -> int | None:
        """Aplica o `limit` máximo configurado para o schema (seção 2.6)."""
        if self.max_limit is None:
            return requested
        if requested is None:
            return self.max_limit
        return min(requested, self.max_limit)

    def columns_for(self, request: QueryRequest) -> tuple[Column, ...]:
        """Colunas da resposta, na ordem pedida — medidas são sempre `number` (seção 2.3)."""
        columns = [
            Column(field=name, type=self.dimensions[name].type) for name in request.dimensions
        ]
        columns += [
            Column(field=name, type=DataType.NUMBER, format=self.measures[name].format)
            for name in request.measures
        ]
        return tuple(columns)

    def get_dataset(self, name: str) -> Dataset:
        """Um dos `datasets` do schema, pelo nome.

        Usado pelo worker da fila (Marco 7): o job carrega o **nome** do dataset já
        escolhido por `ResolveDataset` no momento do enfileiramento (seção 2.4), então o
        worker não resolve de novo — só busca. `LookupError`, não `DomainError`: um nome
        ausente aqui é publicação de catálogo incompatível com um job antigo na fila, não
        erro de cliente.
        """
        for dataset in self.datasets:
            if dataset.name == name:
                return dataset
        raise LookupError(f"Dataset '{name}' não existe no schema '{self.name}'.")


# --------------------------------------------------------------------------------------
# Catálogo em memória
# --------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Catalog:
    """Cópia em memória das versões ativas de `catalog_versions`, indexada por nome.

    É "o catálogo em memória de cada instância" do CLAUDE.md: quem o povoa a partir do
    `CatalogRepository` é o composition root (Marco 8) — esta entidade não sabe nada
    sobre Postgres nem sobre publicação, só resolve nome → `Schema`.
    """

    schemas: Mapping[str, Schema]

    def __post_init__(self) -> None:
        object.__setattr__(self, "schemas", _freeze(self.schemas))
        for name, schema in self.schemas.items():
            if name != schema.name:
                raise InvalidCatalogError(
                    f"O catálogo indexa '{name}' mas o schema se chama '{schema.name}'.",
                    [name],
                )

    def get_schema(self, name: str) -> Schema:
        """O schema pelo nome, ou `UnknownSchemaError` se não estiver no catálogo."""
        try:
            return self.schemas[name]
        except KeyError:
            raise UnknownSchemaError(
                f"Schema '{name}' não existe no catálogo.", [name]
            ) from None

    def schema_names(self) -> tuple[str, ...]:
        """Nomes dos schemas, na ordem em que foram inseridos — usado por `/v1/catalog`."""
        return tuple(self.schemas)

    def __contains__(self, name: str) -> bool:
        return name in self.schemas


# --------------------------------------------------------------------------------------
# Catálogo publicado
# --------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CatalogVersion:
    """Uma linha da tabela `catalog_versions` (`docs/pipeline-publicacao.md`), já com o
    `Schema` desserializado — a tradução JSON → `Schema` é do adapter (Marco 8).

    Cada publicação insere uma versão nova e desativa a anterior na mesma transação;
    isso é responsabilidade do `CatalogRepository`, não desta entidade.
    """

    schema: Schema
    content_hash: str
    git_sha: str
    published_at: datetime
    is_active: bool = True
    published_by: str | None = None
