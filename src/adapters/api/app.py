"""Fábrica da aplicação FastAPI.

`create_app` recebe as peças já construídas em vez de montá-las: quem lê env vars, abre
engines e popula o catálogo é o composition root do Marco 8 (`main.py`). Assim este
adapter continua testável com os fakes do Marco 2, sem banco nenhum.
"""

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError

from adapters.api.errors import domain_error_handler, validation_error_handler
from adapters.api.routers import catalog as catalog_router
from adapters.api.routers import query as query_router
from application.ports.job_queue import JobQueue
from application.use_cases.execute_query import ExecuteQuery
from domain.errors import DomainError
from domain.models import Catalog


def create_app(
    *, catalog: Catalog, execute_query: ExecuteQuery, job_queue: JobQueue
) -> FastAPI:
    app = FastAPI(
        title="API de consultas analíticas multi-banco",
        version="1.0.0",
        description=(
            "Consultas analíticas sobre um modelo lógico versionado em catálogo. "
            "O dataset físico que atende cada requisição é escolhido pelo servidor."
        ),
    )

    app.state.catalog = catalog
    app.state.execute_query = execute_query
    app.state.job_queue = job_queue

    # Todo erro de domínio vira o envelope único da seção 2.5; `DomainError` é a base
    # de todos eles, então um handler só cobre a hierarquia inteira.
    app.add_exception_handler(DomainError, domain_error_handler)
    app.add_exception_handler(RequestValidationError, validation_error_handler)

    app.include_router(catalog_router.router)
    app.include_router(query_router.router)
    return app
