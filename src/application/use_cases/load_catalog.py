"""Reconstrói o `Catalog` em memória a partir das versões ativas do repositório.

Usado em três momentos (Marco 8): no boot do processo, no endpoint
`POST /internal/catalog/reload` e no callback do assinante de pub/sub — os três fazem
exatamente a mesma coisa, então viram um único use case em vez de três cópias.
"""

from application.ports.catalog_repository import CatalogRepository
from domain.models import Catalog


class LoadCatalog:
    def __init__(self, repository: CatalogRepository) -> None:
        self._repository = repository

    async def __call__(self) -> Catalog:
        versions = await self._repository.list_active_versions()
        return Catalog(schemas={version.schema.name: version.schema for version in versions})
