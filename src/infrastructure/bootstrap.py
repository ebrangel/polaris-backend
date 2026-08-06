"""Monta a aplicação FastAPI e o `WorkerSettings` do worker `arq` a partir de env vars
(`infrastructure/config.py`, `infrastructure/db.py`).

Separado de `main.py` (que fica só com o `if PROCESS_ROLE == ...`) de propósito:
`main.py` executa a montagem como efeito colateral do import (é o que permite
`uvicorn main:app`/`arq main.WorkerSettings`), então nada mais pode importar `main.py`
sem pagar esse custo. `scripts/publish_catalog.py` importa só deste módulo —
`build_context`/`build_inspectors` sem nenhum efeito colateral de import.
"""

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass

from arq.connections import RedisSettings, create_pool
from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from adapters.api import create_app
from adapters.cache.redis_cache import RedisCacheGateway
from adapters.cache.redis_pubsub import RedisCatalogInvalidator, listen_for_invalidation
from adapters.cache.redis_rate_limiter import RedisRateLimiter
from adapters.catalog.postgres_inspector import PostgresInspector
from adapters.executors import ElasticsearchQueryExecutor, SQLAlchemyQueryExecutor
from adapters.queue.arq_queue import ArqJobQueue
from adapters.queue.tasks import build_worker_settings
from adapters.repositories.postgres_catalog_repository import PostgresCatalogRepository
from application.ports.catalog_repository import CatalogRepository
from application.ports.datasource_inspector import DatasourceInspector
from application.ports.query_executor import QueryExecutor
from application.use_cases import (
    ExecuteQuery,
    GetObservabilitySnapshot,
    LoadCatalog,
    PublishCatalog,
    ResolveDataset,
    RunQueuedQuery,
)
from domain.models import Catalog, DatasourceType
from infrastructure import db
from infrastructure.config import Settings, load_settings


def _configure_logging(settings: Settings) -> None:
    """Chamado uma vez por processo (API ou worker) — sem isso, o log de consultas
    lentas (Marco 9) roda para um logger sem handler nenhum, e nada aparece."""
    logging.basicConfig(level=settings.log_level)


@dataclass
class ApplicationContext:
    """Peças compartilhadas entre a API e o worker — construídas uma vez no boot."""

    settings: Settings
    catalog: Catalog
    catalog_repository: CatalogRepository
    catalog_engine: AsyncEngine
    executors: dict[str, QueryExecutor]
    relational_engines: dict[str, db.EnginePair]
    es_clients: dict[str, AsyncElasticsearch]

    async def dispose(self) -> None:
        await db.dispose_relational_engines(self.relational_engines)
        await db.close_elasticsearch_clients(self.es_clients)
        await self.catalog_engine.dispose()


async def build_context(settings: Settings) -> ApplicationContext:
    """Lê o catálogo ativo do Postgres e monta um `QueryExecutor` por `connection_ref`
    do catálogo — Postgres/Oracle via `SQLAlchemyQueryExecutor`, Elasticsearch via
    `ElasticsearchQueryExecutor`, cada um com seu próprio par de engines leve/pesado
    (`docs/escalabilidade.md`)."""
    catalog_engine = create_async_engine(settings.catalog_db_url)
    catalog_repository = PostgresCatalogRepository(catalog_engine)
    catalog = await LoadCatalog(catalog_repository)()

    schemas = list(catalog.schemas.values())
    relational_engines = db.build_relational_engines(
        schemas,
        light_pool_size=settings.light_pool_size,
        heavy_pool_size=settings.heavy_pool_size,
    )
    es_clients = db.build_elasticsearch_clients(schemas)

    executors: dict[str, QueryExecutor] = {}
    for connection_ref, engines in relational_engines.items():
        executors[connection_ref] = SQLAlchemyQueryExecutor(
            light_engine=engines.light,
            heavy_engine=engines.heavy,
            light_timeout_seconds=settings.light_timeout_seconds,
            heavy_timeout_seconds=settings.heavy_timeout_seconds,
            cost_threshold=settings.cost_threshold,
        )
    for connection_ref, client in es_clients.items():
        executors[connection_ref] = ElasticsearchQueryExecutor(
            client=client,
            light_timeout_seconds=settings.light_timeout_seconds,
            heavy_timeout_seconds=settings.heavy_timeout_seconds,
        )

    return ApplicationContext(
        settings=settings,
        catalog=catalog,
        catalog_repository=catalog_repository,
        catalog_engine=catalog_engine,
        executors=executors,
        relational_engines=relational_engines,
        es_clients=es_clients,
    )


def build_inspectors(context: ApplicationContext) -> dict[str, DatasourceInspector]:
    """Só Postgres tem `DatasourceInspector` real neste marco (Oracle não tem
    container nos testes deste projeto, mesma limitação desde o Marco 5) — os demais
    `connection_ref` ficam de fora do mapa e `PublishCatalog` os reporta como não
    inspecionados, em vez de falhar."""
    types = db.datasource_types(context.catalog.schemas.values())
    return {
        connection_ref: PostgresInspector(engines.light)
        for connection_ref, engines in context.relational_engines.items()
        if types.get(connection_ref) is DatasourceType.POSTGRES
    }


def _build_lifespan(
    context: ApplicationContext,
) -> Callable[[FastAPI], "contextlib._AsyncGeneratorContextManager[None]"]:
    """Assina `catalog:invalidate` durante toda a vida do processo e troca
    `app.state.catalog` a cada evento — "recarreguem o catálogo em memória
    imediatamente, em vez de depender de polling periódico" (`docs/pipeline-publicacao.md`).
    """

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        sub_client = Redis.from_url(context.settings.redis_url, decode_responses=True)
        load_catalog = LoadCatalog(context.catalog_repository)

        async def on_invalidate(schema_name: str) -> None:
            app.state.catalog = await load_catalog()

        task = asyncio.create_task(listen_for_invalidation(sub_client, on_invalidate))
        try:
            yield
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            await sub_client.aclose()
            await context.dispose()

    return lifespan


async def create_application() -> FastAPI:
    settings = load_settings()
    _configure_logging(settings)
    context = await build_context(settings)

    cache_client = Redis.from_url(settings.redis_url, decode_responses=True)
    cache = RedisCacheGateway(cache_client, default_ttl_seconds=settings.cache_ttl_seconds)

    job_queue_pool = await create_pool(RedisSettings.from_dsn(settings.redis_url))
    job_queue = ArqJobQueue(job_queue_pool)

    # Duas instâncias do mesmo adapter, config diferente — mesmo padrão de
    # `executors`, um port com várias instâncias por chave (Marco 9).
    request_rate_limiter = RedisRateLimiter(
        cache_client,
        limit=settings.request_rate_limit,
        window_seconds=settings.request_rate_limit_window_seconds,
        key_prefix="ratelimit:request:",
    )
    heavy_query_rate_limiter = RedisRateLimiter(
        cache_client,
        limit=settings.heavy_query_rate_limit,
        window_seconds=settings.heavy_query_rate_limit_window_seconds,
        key_prefix="ratelimit:heavy:",
    )

    execute_query = ExecuteQuery(
        catalog=context.catalog,
        resolve_dataset=ResolveDataset(),
        executors=context.executors,
        cache=cache,
        job_queue=job_queue,
        cache_ttl_seconds=settings.cache_ttl_seconds,
        request_rate_limiter=request_rate_limiter,
        heavy_query_rate_limiter=heavy_query_rate_limiter,
        max_heavy_queue_depth=settings.max_heavy_queue_depth,
        slow_query_threshold_ms=settings.slow_query_threshold_ms,
    )

    invalidator = RedisCatalogInvalidator(
        Redis.from_url(settings.redis_url, decode_responses=True)
    )
    publish_catalog = PublishCatalog(
        repository=context.catalog_repository,
        inspectors=build_inspectors(context),
        invalidator=invalidator,
    )

    get_observability_snapshot = GetObservabilitySnapshot(cache=cache, job_queue=job_queue)

    return create_app(
        catalog=context.catalog,
        execute_query=execute_query,
        job_queue=job_queue,
        publish_catalog=publish_catalog,
        catalog_repository=context.catalog_repository,
        get_observability_snapshot=get_observability_snapshot,
        internal_token=settings.internal_token,
        lifespan=_build_lifespan(context),
    )


async def create_worker_settings() -> type:
    settings = load_settings()
    _configure_logging(settings)
    context = await build_context(settings)
    run_queued_query = RunQueuedQuery(
        catalog=context.catalog,
        executors=context.executors,
        slow_query_threshold_ms=settings.slow_query_threshold_ms,
    )
    redis_settings = RedisSettings.from_dsn(settings.redis_url)
    return build_worker_settings(run_queued_query, redis_settings)
