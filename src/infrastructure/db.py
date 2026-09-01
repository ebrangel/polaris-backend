"""Engines/clientes por `connection_ref` — "criação dos engines SQLAlchemy por
datasource" (CLAUDE.md). Um `AsyncEngine` por `connection_ref` **relacional** (nunca
por `DatasourceType`: dois datasets Postgres distintos, como `env:DW_VENDAS_PG_URL` e
`env:APP_ESTOQUE_URL`, são bancos diferentes e não podem compartilhar pool —
`docs/escalabilidade.md`); um `AsyncElasticsearch` por `connection_ref` de
Elasticsearch.

Construir o `QueryExecutor` a partir de cada engine/cliente é do composition root
(`main.py`), que também conhece o timeout por datasource (`Settings`). Só o processo
worker abre esses engines para executar consulta; a API os constrói apenas para o
`DatasourceInspector` da publicação de catálogo.
"""

from collections.abc import Iterable

from elasticsearch import AsyncElasticsearch
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from domain.models import Dataset, DatasourceType, Schema
from infrastructure.config import resolve_connection_ref


def _representative_datasets(schemas: Iterable[Schema]) -> dict[str, Dataset]:
    """Um dataset por `connection_ref` — o suficiente para saber o `DatasourceType`;
    todo dataset com o mesmo `connection_ref` aponta para o mesmo datasource físico."""
    by_ref: dict[str, Dataset] = {}
    for schema in schemas:
        for dataset in schema.datasets:
            by_ref.setdefault(dataset.datasource.connection_ref, dataset)
    return by_ref


def build_relational_engines(
    schemas: Iterable[Schema],
    *,
    pool_size: int = 10,
) -> dict[str, AsyncEngine]:
    """Um `AsyncEngine` por `connection_ref` cujo datasource é Postgres/Oracle."""
    engines: dict[str, AsyncEngine] = {}
    for connection_ref, dataset in _representative_datasets(schemas).items():
        if dataset.datasource.type is DatasourceType.ELASTICSEARCH:
            continue
        url = resolve_connection_ref(connection_ref)
        engines[connection_ref] = create_async_engine(url, pool_size=pool_size)
    return engines


def build_elasticsearch_clients(schemas: Iterable[Schema]) -> dict[str, AsyncElasticsearch]:
    """Um `AsyncElasticsearch` por `connection_ref` cujo datasource é Elasticsearch."""
    clients: dict[str, AsyncElasticsearch] = {}
    for connection_ref, dataset in _representative_datasets(schemas).items():
        if dataset.datasource.type is DatasourceType.ELASTICSEARCH:
            clients[connection_ref] = AsyncElasticsearch(
                hosts=[resolve_connection_ref(connection_ref)]
            )
    return clients


async def dispose_relational_engines(engines: dict[str, AsyncEngine]) -> None:
    for engine in engines.values():
        await engine.dispose()


async def close_elasticsearch_clients(clients: dict[str, AsyncElasticsearch]) -> None:
    for client in clients.values():
        await client.close()


def datasource_types(schemas: Iterable[Schema]) -> dict[str, DatasourceType]:
    """`connection_ref` → `DatasourceType` — usado pelo composition root (`main.py`)
    para decidir quais `connection_ref`s ganham um `DatasourceInspector` (só Postgres
    tem implementação real neste marco; ver `adapters/catalog/postgres_inspector.py`).
    """
    return {
        ref: dataset.datasource.type
        for ref, dataset in _representative_datasets(schemas).items()
    }
