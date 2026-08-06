"""`DatasourceInspector` real para Postgres, via `information_schema.columns`.

Só Postgres: Oracle não tem container nos testes deste projeto (mesma limitação
assumida desde o Marco 5) — datasets Oracle simplesmente não têm inspector no mapa que
`PublishCatalog` recebe, e são reportados como não inspecionados, não como erro.
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from domain.models import Dataset, StarModel, TableModel


def _split_source(source: str) -> tuple[str | None, str]:
    schema, sep, name = source.rpartition(".")
    return (schema or None) if sep else None, name


def _table_column_pairs(dataset: Dataset) -> list[tuple[str, str | None, str, str]]:
    """(nome_lógico, schema, tabela, coluna) para cada campo do mapping físico."""
    pairs: list[tuple[str, str | None, str, str]] = []
    model = dataset.model

    if isinstance(model, TableModel):
        schema, table = _split_source(model.source)
        for logical, mapping in model.mapping.items():
            pairs.append((logical, schema, table, mapping.column))
    elif isinstance(model, StarModel):
        fact_schema, fact_table = _split_source(model.fact.table)
        for logical, mapping in model.fact.mapping.items():
            pairs.append((logical, fact_schema, fact_table, mapping.column))
        for dimension_table in model.dimension_tables.values():
            dim_schema, dim_table = _split_source(dimension_table.table)
            for logical, mapping in dimension_table.mapping.items():
                pairs.append((logical, dim_schema, dim_table, mapping.column))

    return pairs


# Duas queries, não uma com `:table_schema IS NULL OR table_schema = :table_schema`:
# o protocolo estendido do psycopg (async) prepara a query no servidor antes de
# enviar os parâmetros, e o planner do Postgres não consegue inferir o tipo de um
# parâmetro usado só dentro de `IS NULL OR ...` (`AmbiguousParameter`). Quando o
# catálogo não declara schema (`table_schema=None`), casa em qualquer schema do
# `search_path` — é o que "sem schema" significa numa tabela sem prefixo no YAML.
_COLUMN_EXISTS_IN_SCHEMA = text(
    "SELECT 1 FROM information_schema.columns "
    "WHERE table_schema = :table_schema AND table_name = :table_name "
    "AND column_name = :column_name"
)
_COLUMN_EXISTS_ANY_SCHEMA = text(
    "SELECT 1 FROM information_schema.columns "
    "WHERE table_name = :table_name AND column_name = :column_name"
)


class PostgresInspector:
    """Implementa `DatasourceInspector` consultando o catálogo do próprio Postgres."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def missing_fields(self, dataset: Dataset) -> tuple[str, ...]:
        missing: list[str] = []
        async with self._engine.connect() as conn:
            for logical_name, schema, table, column in _table_column_pairs(dataset):
                if schema is not None:
                    query, params = _COLUMN_EXISTS_IN_SCHEMA, {
                        "table_schema": schema,
                        "table_name": table,
                        "column_name": column,
                    }
                else:
                    query, params = _COLUMN_EXISTS_ANY_SCHEMA, {
                        "table_name": table,
                        "column_name": column,
                    }
                result = await conn.execute(query, params)
                if result.first() is None:
                    missing.append(logical_name)
        return tuple(missing)
