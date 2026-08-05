"""Dependências injetadas nos routers.

As instâncias concretas ficam em `app.state`, preenchidas pela fábrica `create_app`.
O composition root de verdade (`main.py`, lendo env vars e abrindo engines) é do
Marco 8 — aqui só existe o ponto de injeção, que os testes preenchem com os fakes do
Marco 2.
"""

from typing import Annotated

from fastapi import Depends, Header, Request

from application.ports.job_queue import JobQueue
from application.use_cases.execute_query import ExecuteQuery
from domain.models import Catalog


def get_catalog(request: Request) -> Catalog:
    return request.app.state.catalog


def get_execute_query(request: Request) -> ExecuteQuery:
    return request.app.state.execute_query


def get_job_queue(request: Request) -> JobQueue:
    return request.app.state.job_queue


def get_roles(x_roles: Annotated[str | None, Header()] = None) -> tuple[str, ...]:
    """Roles do chamador, para o controle de acesso da seção 2.5 (`forbidden_measure`).

    **Provisório**: lê o header `X-Roles` (lista separada por vírgula) porque o projeto
    ainda não tem autenticação — nenhum marco a introduziu até aqui. O importante é que
    o caminho HTTP → use case já existe e é explícito; trocar a origem dos roles por um
    token verificado é mudança local a esta função.
    """
    if not x_roles:
        return ()
    return tuple(role.strip() for role in x_roles.split(",") if role.strip())


CatalogDep = Annotated[Catalog, Depends(get_catalog)]
ExecuteQueryDep = Annotated[ExecuteQuery, Depends(get_execute_query)]
JobQueueDep = Annotated[JobQueue, Depends(get_job_queue)]
RolesDep = Annotated[tuple[str, ...], Depends(get_roles)]
