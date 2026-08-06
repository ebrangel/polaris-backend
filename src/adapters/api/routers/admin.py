"""`/internal/catalog/publish` e `/internal/catalog/reload` — o lado HTTP do pipeline
de `docs/pipeline-publicacao.md`.

Protegidas por `require_internal_token` (registrado como dependência do router
inteiro): nenhuma rota aqui responde sem o header `X-Internal-Token` batendo com o
que o composition root configurou.
"""

from typing import Any

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, ConfigDict

from adapters.api.dependencies import (
    CatalogRepositoryDep,
    PublishCatalogDep,
    require_internal_token,
)
from application.use_cases import LoadCatalog

router = APIRouter(
    prefix="/internal/catalog",
    tags=["admin"],
    dependencies=[Depends(require_internal_token)],
)


class PublishRequestModel(BaseModel):
    """Corpo de `POST /internal/catalog/publish`.

    `data` é o schema já em `dict` — o mesmo formato que `yaml.safe_load` produz a
    partir de um arquivo em `catalog/schemas/`. `git_sha` e `published_by` viajam
    junto porque `CatalogRepository.publish_new_version` os grava na linha (seção
    "Tabela no banco" de `docs/pipeline-publicacao.md`).
    """

    model_config = ConfigDict(extra="forbid")

    data: dict[str, Any]
    git_sha: str
    published_by: str | None = None


@router.post("/publish")
async def publish(body: PublishRequestModel, publish_catalog: PublishCatalogDep) -> dict[str, Any]:
    outcome = await publish_catalog(
        body.data, git_sha=body.git_sha, published_by=body.published_by
    )
    return {
        "published": outcome.published,
        "schema_name": outcome.schema_name,
        "reason": outcome.reason,
        "uninspected_datasets": list(outcome.uninspected_datasets),
    }


@router.post("/reload")
async def reload_catalog(
    request: Request, catalog_repository: CatalogRepositoryDep
) -> dict[str, Any]:
    """Recarrega o catálogo em memória **desta instância** a partir do repositório.

    O caminho automático entre réplicas é o pub/sub (Marco 8); este endpoint é o
    caminho manual — útil em teste e para forçar uma recarga sem esperar o evento.
    """
    load_catalog = LoadCatalog(catalog_repository)
    new_catalog = await load_catalog()
    request.app.state.catalog = new_catalog
    return {"reloaded": True, "schemas": list(new_catalog.schema_names())}
