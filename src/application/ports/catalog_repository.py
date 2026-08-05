"""Port de acesso ao catálogo publicado (`docs/pipeline-publicacao.md`).

O catálogo em memória de cada instância é uma cópia da tabela `catalog_versions`
(Postgres) — o `CatalogRepository` é a fronteira entre essa tabela e o domínio.
Nunca lê o YAML do git em tempo de execução (regra do CLAUDE.md).
"""

from typing import Protocol, runtime_checkable

from domain.models import CatalogVersion


@runtime_checkable
class CatalogRepository(Protocol):
    """Leitura e publicação de versões do catálogo."""

    async def get_active_version(self, schema_name: str) -> CatalogVersion | None:
        """Versão atualmente ativa de um schema, ou `None` se ele não existir."""
        ...

    async def list_active_versions(self) -> tuple[CatalogVersion, ...]:
        """Todas as versões ativas — usado para popular o catálogo em memória."""
        ...

    async def publish_new_version(
        self,
        schema_name: str,
        content: str,
        content_hash: str,
        git_sha: str,
        published_by: str | None = None,
    ) -> CatalogVersion:
        """Insere uma nova versão e desativa a anterior, na mesma transação.

        `content` é o JSON canônico já compilado do schema (não o YAML bruto); a
        desserialização `content` → `Schema` é responsabilidade do adapter, não deste
        port. Nunca faz `UPDATE` em `content` — cada publicação é uma linha nova,
        preservando o histórico completo.
        """
        ...
