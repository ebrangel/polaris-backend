"""`/v1/catalog` e `/v1/catalog/{schema}` (seção 2.1).

"Expor `/v1/catalog` é importante: permite que quem consome a API descubra
dinamicamente o que pode consultar, sem depender de documentação separada."
"""

from typing import Any

from fastapi import APIRouter

from adapters.api.dependencies import CatalogDep
from adapters.api.schemas import present_schema_detail, present_schema_summary

router = APIRouter(prefix="/v1/catalog", tags=["catalog"])


@router.get("")
async def list_schemas(catalog: CatalogDep) -> dict[str, Any]:
    """Schemas disponíveis, na ordem em que o catálogo foi carregado."""
    return {
        "schemas": [
            present_schema_summary(catalog.get_schema(name)) for name in catalog.schema_names()
        ]
    }


@router.get("/{schema_name}")
async def get_schema_detail(schema_name: str, catalog: CatalogDep) -> dict[str, Any]:
    """Dimensões e medidas do modelo lógico — `UnknownSchemaError` (404) se não existir."""
    return present_schema_detail(catalog.get_schema(schema_name))
