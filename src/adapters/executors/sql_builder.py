"""Traduz `Dataset` (mapping do catálogo) + `QueryRequest` (nomes lógicos) + `columns`
(de `Schema.columns_for`) num `Select` do SQLAlchemy Core — puro, sem I/O, sem conexão.

As `Table` são construídas em memória a partir do `mapping` do catálogo, nunca
refletidas de um banco real: "todo SQL nasce do query builder... validado contra o
catálogo carregado em memória" (CLAUDE.md). Modelo plano (`TableModel`) e star schema
(`StarModel`) compartilham o mesmo caminho de código a partir de `_resolve_physical` —
plano é só "fato sem joins", como a seção 1.2 do documento descreve.
"""

from collections.abc import Mapping
from dataclasses import dataclass

import sqlalchemy as sa
from sqlalchemy.sql import ColumnElement, FromClause, Select

from domain.models import (
    Aggregation,
    Column as DomainColumn,
    ColumnMapping,
    Dataset,
    QueryRequest,
    SortDirection,
    StarModel,
    TableModel,
)

#: Rótulo da coluna auxiliar de `COUNT(*) OVER ()`. Prefixo `__` para não colidir com
#: nome lógico de campo algum: o catálogo usa nomes como `sigla_uf`, e um campo que
#: começasse com dois sublinhados nem passaria pela validação do schema.
TOTAL_ROWS_LABEL = "__total_rows"

_AGG_FUNCS = {
    Aggregation.SUM: sa.func.sum,
    Aggregation.AVG: sa.func.avg,
    Aggregation.VALUE_COUNT: sa.func.count,
    Aggregation.COUNT: sa.func.count,
    Aggregation.MIN: sa.func.min,
    Aggregation.MAX: sa.func.max,
}

_FILTER_OPS = {
    "eq": lambda col, val: col == val,
    "neq": lambda col, val: col != val,
    "in": lambda col, val: col.in_(val),
    "between": lambda col, val: col.between(val[0], val[1]),
    "gt": lambda col, val: col > val,
    "gte": lambda col, val: col >= val,
    "lt": lambda col, val: col < val,
    "lte": lambda col, val: col <= val,
    "contains": lambda col, val: col.contains(val),
}


@dataclass(frozen=True)
class _Physical:
    """FROM clause já resolvido (tabela única ou join) + coluna física por nome lógico."""

    from_clause: FromClause
    columns: Mapping[str, ColumnElement]


def _split_source(source: str) -> tuple[str | None, str]:
    schema, sep, name = source.rpartition(".")
    return (schema or None) if sep else None, name


def _build_table(
    metadata: sa.MetaData,
    source: str,
    mapping: Mapping[str, ColumnMapping],
    extra_columns: frozenset[str] = frozenset(),
) -> sa.Table:
    """Uma `Table` em memória com as colunas do `mapping` + colunas físicas extras (ex:
    chave primária/estrangeira de join, que não têm nome lógico no modelo).

    Sem tipo declarado (`NullType`): o driver determina o tipo dos valores de retorno a
    partir dos metadados do próprio servidor, não do que é declarado aqui; para bind
    parameters, o driver adapta o valor Python diretamente. Não há necessidade de
    refletir tipos reais de coluna para este adapter funcionar.
    """
    schema, name = _split_source(source)
    physical_names = {m.column for m in mapping.values()} | set(extra_columns)
    columns = [sa.Column(physical_name, sa.types.NullType()) for physical_name in physical_names]
    return sa.Table(name, metadata, *columns, schema=schema)


def _resolve_physical(dataset: Dataset) -> _Physical:
    metadata = sa.MetaData()
    model = dataset.model

    if isinstance(model, TableModel):
        table = _build_table(metadata, model.source, model.mapping)
        lookup = {logical: table.c[m.column] for logical, m in model.mapping.items()}
        return _Physical(from_clause=table, columns=lookup)

    assert isinstance(model, StarModel)
    fact_mapping = model.fact.mapping
    fk_columns = frozenset(key.column for key in model.fact.keys.values())
    fact_table = _build_table(metadata, model.fact.table, fact_mapping, extra_columns=fk_columns)
    lookup: dict[str, ColumnElement] = {
        logical: fact_table.c[m.column] for logical, m in fact_mapping.items()
    }

    from_clause: FromClause = fact_table
    for alias, dim in model.dimension_tables.items():
        dim_table = _build_table(
            metadata, dim.table, dim.mapping, extra_columns=frozenset({dim.primary_key})
        )
        for logical, m in dim.mapping.items():
            lookup[logical] = dim_table.c[m.column]

        # A junção usa `fact.keys` (coluna física da FK + alias da dimensão), não a
        # seção `joins:` do YAML — que só carrega `to_alias`, já validado no Marco 1;
        # `from_ref` nunca teve um alias declarado no documento (inconsistência da
        # própria seção 1.0) e não é usado para nada funcional.
        key = next(k for k in model.fact.keys.values() if k.dimension_alias == alias)
        from_clause = from_clause.join(
            dim_table, fact_table.c[key.column] == dim_table.c[dim.primary_key]
        )

    return _Physical(from_clause=from_clause, columns=lookup)


def _column_expr(logical_name: str, dataset: Dataset, physical: _Physical) -> ColumnElement:
    col = physical.columns[logical_name]
    if logical_name not in dataset.provides.measures:
        return col.label(logical_name)

    mapping = dataset.physical_for(logical_name)
    assert isinstance(mapping, ColumnMapping)
    if mapping.agg is Aggregation.COUNT_DISTINCT:
        return sa.func.count(sa.distinct(col)).label(logical_name)
    return _AGG_FUNCS[mapping.agg](col).label(logical_name)


def needs_window_count(request: QueryRequest) -> bool:
    """Se a projeção leva a coluna `COUNT(*) OVER ()` desta vez.

    A janela obriga o banco a apurar o resultado inteiro antes de devolver a primeira
    linha — o `LIMIT` deixa de economizar trabalho. Por isso ela só entra quando o total
    não sai de graça:

    - `offset == 0` e vieram menos linhas que o `limit`: o total **é** o número de linhas
      lidas, e nada precisa ser pedido ao banco;
    - `offset == 0` e o resultado bateu no `limit`: pode haver mais, mas isso só se sabe
      depois de drenar — aí o executor pede `build_count()`;
    - `offset > 0`: qualquer contagem local seria parcial, então a janela entra no próprio
      SELECT e o total sai na mesma passada.

    Note que só o terceiro caso é decidível aqui, na construção do SQL; os dois primeiros
    dependem de quantas linhas vieram.
    """
    return request.offset > 0


def _build_core(
    dataset: Dataset, request: QueryRequest, columns: tuple[DomainColumn, ...]
) -> tuple[Select, dict[str, ColumnElement]]:
    """A consulta sem paginação nem coluna de janela — o que `build_select` e
    `build_count` têm em comum."""
    physical = _resolve_physical(dataset)

    field_names = [c.field for c in columns]
    exprs = {name: _column_expr(name, dataset, physical) for name in field_names}

    stmt = sa.select(*(exprs[name] for name in field_names)).select_from(physical.from_clause)

    conditions = [
        _FILTER_OPS[f.operator.value](physical.columns[f.field], f.value) for f in request.filters
    ]
    if conditions:
        stmt = stmt.where(sa.and_(*conditions))

    measure_fields = {name for name in field_names if name in dataset.provides.measures}
    dimension_fields = [name for name in field_names if name not in measure_fields]
    if measure_fields and dimension_fields:
        stmt = stmt.group_by(*(exprs[name] for name in dimension_fields))

    return stmt, exprs


def _apply_order_by(
    stmt: Select,
    dataset: Dataset,
    request: QueryRequest,
    exprs: Mapping[str, ColumnElement],
) -> Select:
    physical = _resolve_physical(dataset)
    order_exprs = []
    for order in request.order_by:
        # `or` não serve aqui: ColumnElement sobrescreve __bool__ para levantar
        # TypeError (é usado para montar expressões SQL, não para testar verdade).
        expr = exprs[order.field] if order.field in exprs else _column_expr(
            order.field, dataset, physical
        )
        order_exprs.append(expr.desc() if order.direction is SortDirection.DESC else expr.asc())
    if order_exprs:
        stmt = stmt.order_by(*order_exprs)
    return stmt


def build_select(dataset: Dataset, request: QueryRequest, columns: tuple[DomainColumn, ...]) -> Select:
    """Monta o `Select` completo: projeção (na ordem de `columns`), `WHERE`, `GROUP BY`
    (só quando há medida pedida), `ORDER BY`, `LIMIT`/`OFFSET`.

    Quando `needs_window_count(request)`, a projeção ganha uma coluna extra
    `COUNT(*) OVER () AS __total_rows` no fim. Ela **não** entra em `columns`, e por isso
    fica de fora de `GROUP BY` e `ORDER BY`, que são derivados de `columns` e não do
    `Select` — o que é exatamente o certo: com `GROUP BY`, a janela conta os grupos, ou
    seja, as linhas do resultado. O executor retira essa coluna de cada linha antes de
    entregá-la ao sink.
    """
    stmt, exprs = _build_core(dataset, request, columns)

    if needs_window_count(request):
        stmt = stmt.add_columns(sa.func.count().over().label(TOTAL_ROWS_LABEL))

    stmt = _apply_order_by(stmt, dataset, request, exprs)

    if request.limit is not None:
        stmt = stmt.limit(request.limit)
    return stmt.offset(request.offset)


def build_count(
    dataset: Dataset, request: QueryRequest, columns: tuple[DomainColumn, ...]
) -> Select:
    """`SELECT count(*) FROM (<a mesma consulta, sem paginação>)`.

    É o plano B de `needs_window_count`: serve o caso `offset == 0` em que o resultado
    bateu no `limit` e portanto pode estar truncado — situação que só se conhece depois
    de ler as linhas, tarde demais para acrescentar uma coluna ao SELECT.

    A subconsulta sai sem `ORDER BY`: ordenar para depois contar é trabalho jogado fora, e
    alguns bancos recusam `ORDER BY` em subconsulta sem `LIMIT`. Com `GROUP BY`, contar as
    linhas da subconsulta é contar os grupos — a mesma semântica da janela.
    """
    core, _ = _build_core(dataset, request, columns)
    return sa.select(sa.func.count()).select_from(core.subquery())
