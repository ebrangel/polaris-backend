"""Monta a aplicação FastAPI e o `WorkerSettings` do worker `arq` a partir de env vars
(`infrastructure/config.py`, `infrastructure/db.py`).

Separado de `main.py` (que fica só com o `if PROCESS_ROLE == ...`) de propósito:
`main.py` constrói a app/o `WorkerSettings` como efeito colateral do import (é o que
permite `uvicorn main:app`/`arq main.WorkerSettings`), então nada mais pode importar
`main.py` sem pagar esse custo. `scripts/publish_catalog.py` importa só deste módulo —
`build_context`/`build_inspectors` sem nenhum efeito colateral de import.

As duas fábricas (`create_application`, `create_worker_settings`) são síncronas: o
uvicorn importa `main:app` de dentro do event loop dele, onde `asyncio.run()` estoura.
O I/O do boot fica no `lifespan` da app e no `on_startup` do worker.
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
from adapters.exports import LocalFileResultExporter
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
    PurgeCache,
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


def build_result_exporter(settings: Settings) -> LocalFileResultExporter:
    """Construído igual nos dois processos, a partir da mesma configuração: o worker
    escreve e varre, a API lê. É por isso que `EXPORT_DIR` precisa apontar para o mesmo
    lugar nos dois — sem volume compartilhado, a API não acha o que o worker gravou."""
    return LocalFileResultExporter(
        settings.export_dir, ttl_seconds=settings.export_ttl_seconds
    )


@dataclass
class ApplicationContext:
    """Peças compartilhadas entre a API e o worker — construídas uma vez no boot."""

    settings: Settings
    catalog: Catalog
    catalog_repository: CatalogRepository
    catalog_engine: AsyncEngine
    executors: dict[str, QueryExecutor]
    relational_engines: dict[str, AsyncEngine]
    es_clients: dict[str, AsyncElasticsearch]

    async def dispose(self) -> None:
        await db.dispose_relational_engines(self.relational_engines)
        await db.close_elasticsearch_clients(self.es_clients)
        await self.catalog_engine.dispose()


async def build_context(settings: Settings) -> ApplicationContext:
    """Lê o catálogo ativo do Postgres e monta um `QueryExecutor` por `connection_ref`
    do catálogo — Postgres/Oracle via `SQLAlchemyQueryExecutor`, Elasticsearch via
    `ElasticsearchQueryExecutor`, cada um com um engine/cliente por datasource.

    O processo worker usa os executores para executar consulta; a API os constrói
    apenas para alimentar o `DatasourceInspector` da publicação de catálogo
    (`build_inspectors`) — nunca para executar consulta."""
    catalog_engine = create_async_engine(settings.catalog_db_url)
    catalog_repository = PostgresCatalogRepository(catalog_engine)
    catalog = await LoadCatalog(catalog_repository)()

    schemas = list(catalog.schemas.values())
    relational_engines = db.build_relational_engines(
        schemas, pool_size=settings.query_pool_size
    )
    es_clients = db.build_elasticsearch_clients(schemas)

    executors: dict[str, QueryExecutor] = {}
    for connection_ref, engine in relational_engines.items():
        executors[connection_ref] = SQLAlchemyQueryExecutor(
            engine=engine,
            timeout_seconds=settings.query_timeout_seconds,
            chunk_size=settings.fetch_chunk_size,
        )
    for connection_ref, client in es_clients.items():
        executors[connection_ref] = ElasticsearchQueryExecutor(
            client=client,
            timeout_seconds=settings.query_timeout_seconds,
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
        connection_ref: PostgresInspector(engine)
        for connection_ref, engine in context.relational_engines.items()
        if types.get(connection_ref) is DatasourceType.POSTGRES
    }


def _build_lifespan(
    settings: Settings,
) -> Callable[[FastAPI], "contextlib._AsyncGeneratorContextManager[None]"]:
    """Monta tudo o que depende de I/O e preenche `app.state`.

    A montagem inteira mora aqui, e não no corpo de `create_application`, porque ela é
    assíncrona (lê o catálogo ativo do Postgres, abre o pool do Redis) e o entry point
    `uvicorn main:app` importa `main.py` **já dentro do event loop do servidor** — um
    `asyncio.run()` no import estoura com "cannot be called from a running event loop".
    O `lifespan` é o gancho do FastAPI feito para isso: roda no loop do servidor, antes
    da primeira requisição, e o `finally` fecha na ordem inversa o que foi aberto.

    Além da montagem, assina `catalog:invalidate` durante toda a vida do processo e
    troca `app.state.catalog` a cada evento — "recarreguem o catálogo em memória
    imediatamente, em vez de depender de polling periódico" (`docs/pipeline-publicacao.md`).
    """

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        context = await build_context(settings)

        cache_client = Redis.from_url(settings.redis_url, decode_responses=True)
        cache = RedisCacheGateway(
            cache_client,
            default_ttl_seconds=settings.cache_ttl_seconds,
            max_rows=settings.cache_max_rows,
            max_payload_bytes=settings.cache_max_payload_bytes,
        )

        job_queue_pool = await create_pool(RedisSettings.from_dsn(settings.redis_url))
        job_queue = ArqJobQueue(
            job_queue_pool, result_poll_delay=settings.inline_wait_poll_delay
        )

        request_rate_limiter = RedisRateLimiter(
            cache_client,
            limit=settings.request_rate_limit,
            window_seconds=settings.request_rate_limit_window_seconds,
            key_prefix="ratelimit:request:",
        )

        pubsub_client = Redis.from_url(settings.redis_url, decode_responses=True)
        invalidator_client = Redis.from_url(settings.redis_url, decode_responses=True)

        app.state.catalog = context.catalog
        app.state.catalog_repository = context.catalog_repository
        app.state.job_queue = job_queue
        app.state.result_exporter = build_result_exporter(settings)
        # `context.executors` não entra aqui: a API não executa consulta (só o worker).
        # Os executores/engines de `context` existem apenas para `build_inspectors`
        # (validação da publicação de catálogo).
        app.state.execute_query = ExecuteQuery(
            catalog=context.catalog,
            resolve_dataset=ResolveDataset(),
            cache=cache,
            job_queue=job_queue,
            request_rate_limiter=request_rate_limiter,
            max_queue_depth=settings.max_queue_depth,
            default_max_limit=settings.default_max_limit,
            inline_wait_seconds=settings.inline_wait_seconds,
        )
        app.state.publish_catalog = PublishCatalog(
            repository=context.catalog_repository,
            inspectors=build_inspectors(context),
            invalidator=RedisCatalogInvalidator(invalidator_client),
        )
        app.state.get_observability_snapshot = GetObservabilitySnapshot(
            cache=cache, job_queue=job_queue
        )
        app.state.purge_cache = PurgeCache(cache=cache)

        load_catalog = LoadCatalog(context.catalog_repository)

        async def on_invalidate(schema_name: str) -> None:
            # Uma nova versão do schema pode mudar mapping/joins/medidas — o resultado
            # em cache daquele schema deixa de ser confiável. Recarrega o catálogo em
            # memória e derruba o cache do schema republicado (o `query_id` embute o
            # hash da requisição, não o do catálogo, então o cache não expiraria
            # sozinho por conta da publicação).
            app.state.catalog = await load_catalog()
            await cache.clear(schema_name)

        task = asyncio.create_task(listen_for_invalidation(pubsub_client, on_invalidate))
        try:
            yield
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            await pubsub_client.aclose()
            await invalidator_client.aclose()
            await cache_client.aclose()
            await job_queue_pool.aclose()
            await context.dispose()

    return lifespan


def create_application() -> FastAPI:
    """Composition root da API — **síncrono de propósito**: `uvicorn main:app` importa o
    módulo de dentro do seu event loop, então nada aqui pode ser `await`ado no import.
    O que precisa de I/O vai para o `lifespan` (acima); aqui ficam só a leitura das env
    vars e a construção da app.
    """
    settings = load_settings()
    _configure_logging(settings)
    return create_app(
        internal_token=settings.internal_token,
        include_admin=True,
        include_observability=True,
        include_cache_admin=True,
        lifespan=_build_lifespan(settings),
    )


def create_worker_settings() -> type:
    """Mesma divisão da API: síncrono no import, I/O no `on_startup` do arq — que roda
    no loop do worker, e não num loop temporário que morre logo depois (deixando para
    trás engines com conexões presas a um loop fechado).
    """
    settings = load_settings()
    _configure_logging(settings)

    result_exporter = build_result_exporter(settings)

    async def provide_run_queued_query() -> RunQueuedQuery:
        context = await build_context(settings)
        # O worker é o único escritor do cache de resultados (toda consulta passa pela
        # fila). Cliente Redis dedicado, com os mesmos tetos que a API usava.
        cache_client = Redis.from_url(settings.redis_url, decode_responses=True)
        cache = RedisCacheGateway(
            cache_client,
            default_ttl_seconds=settings.cache_ttl_seconds,
            max_rows=settings.cache_max_rows,
            max_payload_bytes=settings.cache_max_payload_bytes,
        )
        return RunQueuedQuery(
            catalog=context.catalog,
            executors=context.executors,
            slow_query_threshold_ms=settings.slow_query_threshold_ms,
            result_exporter=result_exporter,
            cache=cache,
            cache_ttl_seconds=settings.cache_ttl_seconds,
        )

    return build_worker_settings(
        run_queued_query_provider=provide_run_queued_query,
        redis_settings=RedisSettings.from_dsn(settings.redis_url),
        # O mesmo exportador do use case: quem escreve os arquivos é quem os varre.
        result_exporter=result_exporter,
    )
