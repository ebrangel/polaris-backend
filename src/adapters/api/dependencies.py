"""Dependências injetadas nos routers.

As instâncias concretas ficam em `app.state`, preenchidas pela fábrica `create_app`.
O composition root de verdade (`main.py`, lendo env vars e abrindo engines) é do
Marco 8 — aqui só existe o ponto de injeção, que os testes preenchem com os fakes do
Marco 2.
"""

from typing import Annotated

from fastapi import Depends, Header, HTTPException, Query, Request

from adapters.api.content_negotiation import OutputFormat, resolve_output_format
from application.ports.catalog_repository import CatalogRepository
from application.ports.job_queue import JobQueue
from application.ports.result_exporter import ResultExporter
from application.use_cases.execute_query import ExecuteQuery
from application.use_cases.get_observability_snapshot import GetObservabilitySnapshot
from application.use_cases.publish_catalog import PublishCatalog
from application.use_cases.purge_cache import PurgeCache
from domain.models import Catalog


def get_catalog(request: Request) -> Catalog:
    return request.app.state.catalog


def get_execute_query(request: Request) -> ExecuteQuery:
    return request.app.state.execute_query


def get_job_queue(request: Request) -> JobQueue:
    return request.app.state.job_queue


def get_result_exporter(request: Request) -> ResultExporter | None:
    """Exportador de resultados pesados (seção 2.4a) — **pode ser `None`**.

    Diferente das demais peças, esta é lida como opcional dentro do próprio router: sem
    exportador configurado não existe export nenhum, então `download_url` some do corpo
    e a rota de download responde `404 export_not_found`, que é a mesma resposta de um
    export que expirou. Um `501` só distinguiria "não configurado" de "não existe" para
    quem opera o servidor, e a informação já está no boot.
    """
    return getattr(request.app.state, "result_exporter", None)


def get_publish_catalog(request: Request) -> PublishCatalog:
    return request.app.state.publish_catalog


def get_catalog_repository(request: Request) -> CatalogRepository:
    return request.app.state.catalog_repository


def get_observability_snapshot_use_case(request: Request) -> GetObservabilitySnapshot:
    return request.app.state.get_observability_snapshot


def get_purge_cache(request: Request) -> PurgeCache:
    return request.app.state.purge_cache


def require_internal_token(
    request: Request, x_internal_token: Annotated[str | None, Header()] = None
) -> None:
    """Protege `/internal/catalog/*` — **stand-in explícito**, no mesmo espírito do
    `X-Roles` de `get_roles`: nenhum marco introduziu autenticação real ainda. Nega por
    omissão — token não configurado no composition root nunca libera acesso.
    """
    expected = getattr(request.app.state, "internal_token", None)
    if not expected or x_internal_token != expected:
        raise HTTPException(status_code=401, detail="Token interno ausente ou inválido.")


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


def get_client_id(
    request: Request, x_api_key: Annotated[str | None, Header()] = None
) -> str:
    """Identidade do cliente para rate limiting (Marco 9) — **stand-in explícito**, no
    mesmo espírito de `get_roles`: lê o header `X-Api-Key` ("chave de API",
    `docs/escalabilidade.md`) porque o projeto ainda não tem autenticação real.

    Nunca devolve uma chave vazia: sem o header, cai no IP do socket — um cliente sem
    `X-Api-Key` ainda é limitado (pelo IP), não fica de fora do rate limiting por
    omissão."""
    if x_api_key:
        return x_api_key
    host = request.client.host if request.client is not None else "desconhecido"
    return f"ip:{host}"


def get_output_format(
    format_: Annotated[
        str | None,
        Query(
            alias="format",
            description=(
                "Formato da resposta: `json` (padrão) ou `csv`. Tem precedência sobre "
                "o header `Accept`. Não faz parte da consulta — não entra no "
                "`query_id` nem na chave de cache."
            ),
        ),
    ] = None,
    accept: Annotated[str | None, Header()] = None,
) -> OutputFormat:
    """Formato de saída negociado (seção 2.3a), para `POST`/`GET /v1/query` e
    `GET /v1/query/{query_id}`.

    É uma dependência, e não um parâmetro de cada rota, para que as três entradas
    negociem do mesmo jeito e o `?format=` apareça no OpenAPI das três.
    """
    return resolve_output_format(format_, accept)


CatalogDep = Annotated[Catalog, Depends(get_catalog)]
ExecuteQueryDep = Annotated[ExecuteQuery, Depends(get_execute_query)]
JobQueueDep = Annotated[JobQueue, Depends(get_job_queue)]
RolesDep = Annotated[tuple[str, ...], Depends(get_roles)]
ClientIdDep = Annotated[str, Depends(get_client_id)]
OutputFormatDep = Annotated[OutputFormat, Depends(get_output_format)]
ResultExporterDep = Annotated[ResultExporter | None, Depends(get_result_exporter)]
PublishCatalogDep = Annotated[PublishCatalog, Depends(get_publish_catalog)]
CatalogRepositoryDep = Annotated[CatalogRepository, Depends(get_catalog_repository)]
ObservabilitySnapshotDep = Annotated[
    GetObservabilitySnapshot, Depends(get_observability_snapshot_use_case)
]
PurgeCacheDep = Annotated[PurgeCache, Depends(get_purge_cache)]
