"""`PublishCatalog` — o núcleo do fluxo de `docs/pipeline-publicacao.md`.

Recebe **um** schema já lido do YAML (o laço sobre `catalog/schemas/*.yaml` é do
script de CI, `scripts/publish_catalog.py`) e segue o pseudocódigo do documento à
risca: compila → compara hash com a versão ativa, saindo cedo se igual → valida contra
o datasource real → publica → invalida o cache das outras réplicas.
"""

from collections.abc import Mapping
from dataclasses import dataclass

from application.catalog_codec import compile_schema
from application.ports.catalog_invalidator import CatalogInvalidator
from application.ports.catalog_repository import CatalogRepository
from application.ports.datasource_inspector import DatasourceInspector
from domain.errors import InvalidCatalogError
from domain.models import CatalogVersion


@dataclass(frozen=True, slots=True)
class PublishOutcome:
    """O que aconteceu ao processar um schema — para o script de CI relatar."""

    published: bool
    schema_name: str
    reason: str | None = None
    version: CatalogVersion | None = None
    uninspected_datasets: tuple[str, ...] = ()


class PublishCatalog:
    def __init__(
        self,
        repository: CatalogRepository,
        inspectors: Mapping[str, DatasourceInspector],
        invalidator: CatalogInvalidator,
    ) -> None:
        """`inspectors` é chaveado por **`connection_ref`**, não por `DatasourceType`
        — mesmo motivo do roteamento de `QueryExecutor` (Marco 4/8): dois datasets
        Postgres distintos (`env:DW_VENDAS_PG_URL`, `env:APP_ESTOQUE_URL`) são bancos
        diferentes, e chavear por engine faria os dois compartilharem inspector."""
        self._repository = repository
        self._inspectors = inspectors
        self._invalidator = invalidator

    async def __call__(
        self, data: Mapping, *, git_sha: str, published_by: str | None = None
    ) -> PublishOutcome:
        # Compila antes de comparar: o hash é do conteúdo compilado, não do YAML cru
        # ("Lógica de publicação incremental", docs/pipeline-publicacao.md) — só assim
        # reordenar chaves ou reindentar o arquivo não dispara uma republicação.
        schema, content, new_hash = compile_schema(data)

        active = await self._repository.get_active_version(schema.name)
        if active is not None and active.content_hash == new_hash:
            return PublishOutcome(
                published=False,
                schema_name=schema.name,
                reason="hash de conteúdo idêntico ao da versão ativa",
            )

        uninspected: list[str] = []
        for dataset in schema.datasets:
            inspector = self._inspectors.get(dataset.datasource.connection_ref)
            if inspector is None:
                # Sem inspector para este connection_ref (ex: Oracle, sem container
                # nos testes deste projeto) — reportado, não é motivo para falhar.
                uninspected.append(dataset.name)
                continue

            missing = await inspector.missing_fields(dataset)
            if missing:
                raise InvalidCatalogError(
                    f"O dataset '{dataset.name}' do schema '{schema.name}' referencia "
                    f"campos sem coluna/campo correspondente no datasource: "
                    f"{', '.join(missing)}.",
                    missing,
                )

        version = await self._repository.publish_new_version(
            schema.name, content, new_hash, git_sha, published_by
        )
        await self._invalidator.publish(schema.name)

        return PublishOutcome(
            published=True,
            schema_name=schema.name,
            version=version,
            uninspected_datasets=tuple(uninspected),
        )
