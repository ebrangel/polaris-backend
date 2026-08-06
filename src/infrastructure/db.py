"""Engines/clientes por `connection_ref` — "criação dos engines SQLAlchemy por
datasource" (CLAUDE.md). Um par leve/pesado por `connection_ref` **relacional**
(nunca por `DatasourceType`: dois datasets Postgres distintos, como
`env:DW_VENDAS_PG_URL` e `env:APP_ESTOQUE_URL`, são bancos diferentes e não podem
compartilhar pool — `docs/escalabilidade.md`); um `AsyncElasticsearch` por
`connection_ref` de Elasticsearch.

Construir o `QueryExecutor` a partir de cada engine/cliente é do composition root
(`main.py`), que também conhece os timeouts/limiar de custo (`Settings`).
"""

from collections.abc import Iterable
from dataclasses import dataclass

from elasticsearch import AsyncElasticsearch
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from domain.models import Dataset, DatasourceType, Schema
from infrastructure.config import resolve_connection_ref


@dataclass(frozen=True, slots=True)
class EnginePair:
    """Os dois pools de um mesmo `connection_ref` — nunca compartilhados entre si."""

    light: AsyncEngine
    heavy: AsyncEngine


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
    light_pool_size: int = 20,
    heavy_pool_size: int = 3,
) -> dict[str, EnginePair]:
    """Um `EnginePair` por `connection_ref` cujo datasource é Postgres/Oracle."""
    engines: dict[str, EnginePair] = {}
    for connection_ref, dataset in _representative_datasets(schemas).items():
        if dataset.datasource.type is DatasourceType.ELASTICSEARCH:
            continue
        url = resolve_connection_ref(connection_ref)
        engines[connection_ref] = EnginePair(
            light=create_async_engine(url, pool_size=light_pool_size),
            heavy=create_async_engine(url, pool_size=heavy_pool_size),
        )
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


async def dispose_relational_engines(engines: dict[str, EnginePair]) -> None:
    for pair in engines.values():
        await pair.light.dispose()
        await pair.heavy.dispose()


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
