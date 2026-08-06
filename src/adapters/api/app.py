"""Fábrica da aplicação FastAPI.

`create_app` recebe as peças já construídas em vez de montá-las: quem lê env vars, abre
engines e popula o catálogo é o composition root (`main.py`, Marco 8). Assim este
adapter continua testável com os fakes do Marco 2, sem banco nenhum.
"""

from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from adapters.api.errors import (
    domain_error_handler,
    http_exception_handler,
    validation_error_handler,
)
from adapters.api.routers import admin as admin_router
from adapters.api.routers import catalog as catalog_router
from adapters.api.routers import observability as observability_router
from adapters.api.routers import query as query_router
from application.ports.catalog_repository import CatalogRepository
from application.ports.job_queue import JobQueue
from application.use_cases.execute_query import ExecuteQuery
from application.use_cases.get_observability_snapshot import GetObservabilitySnapshot
from application.use_cases.publish_catalog import PublishCatalog
from domain.errors import DomainError
from domain.models import Catalog


def create_app(
    *,
    catalog: Catalog,
    execute_query: ExecuteQuery,
    job_queue: JobQueue,
    publish_catalog: PublishCatalog | None = None,
    catalog_repository: CatalogRepository | None = None,
    get_observability_snapshot: GetObservabilitySnapshot | None = None,
    internal_token: str | None = None,
    lifespan: Callable[[FastAPI], AbstractAsyncContextManager[None]] | None = None,
) -> FastAPI:
    """`publish_catalog`/`catalog_repository`/`get_observability_snapshot`/
    `internal_token` são opcionais e aditivos (Marcos 8/9): sem eles, `/internal/*`
    simplesmente não é montado — os testes do contrato de `/v1/*` (Marco 6) continuam
    chamando `create_app` sem conhecer publicação nem observabilidade.

    `lifespan` é onde `main.py` pluga o assinante do pub/sub (`listen_for_invalidation`,
    Marco 8) como uma task de fundo — este adapter não sabe nada sobre Redis, só
    encaminha o gerenciador de contexto para o `FastAPI` de verdade.
    """
    app = FastAPI(
        title="API de consultas analíticas multi-banco",
        version="1.0.0",
        description=(
            "Consultas analíticas sobre um modelo lógico versionado em catálogo. "
            "O dataset físico que atende cada requisição é escolhido pelo servidor."
        ),
        lifespan=lifespan,
    )

    app.state.catalog = catalog
    app.state.execute_query = execute_query
    app.state.job_queue = job_queue
    app.state.publish_catalog = publish_catalog
    app.state.catalog_repository = catalog_repository
    app.state.get_observability_snapshot = get_observability_snapshot
    app.state.internal_token = internal_token

    # Todo erro de domínio vira o envelope único da seção 2.5; `DomainError` é a base
    # de todos eles, então um handler só cobre a hierarquia inteira.
    app.add_exception_handler(DomainError, domain_error_handler)
    app.add_exception_handler(RequestValidationError, validation_error_handler)
    app.add_exception_handler(StarletteHTTPException, http_exception_handler)

    app.include_router(catalog_router.router)
    app.include_router(query_router.router)
    if publish_catalog is not None and catalog_repository is not None:
        app.include_router(admin_router.router)
    if get_observability_snapshot is not None:
        app.include_router(observability_router.router)
    return app
