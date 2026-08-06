"""`PostgresCatalogRepository` — implementa `CatalogRepository` (Marco 2) sobre a
tabela `catalog_versions` de `docs/pipeline-publicacao.md`.

Este banco Postgres é infraestrutura própria do catálogo — "separado de qualquer
datasource analítico que também seja Postgres" (mesmo documento), daí o engine deste
repositório nunca ser um dos engines de `connection_ref` do catálogo.
"""

from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    Boolean,
    Column,
    Identity,
    Index,
    MetaData,
    String,
    Table,
    Text,
    func,
    insert,
    select,
    update,
)
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncEngine

from application.catalog_codec import decompile_schema
from domain.models import CatalogVersion

metadata = MetaData()

#: DDL verbatim de `docs/pipeline-publicacao.md`.
catalog_versions = Table(
    "catalog_versions",
    metadata,
    Column("id", BigInteger, Identity(always=True), primary_key=True),
    Column("schema_name", String(100), nullable=False),
    Column("git_sha", String(40), nullable=False),
    Column("content", Text, nullable=False),  # JSON compilado (inclui a lista de datasets)
    Column("content_hash", String(64), nullable=False),
    Column(
        "published_at", TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    ),
    Column("published_by", String(100)),
    Column("is_active", Boolean, nullable=False, server_default="false"),
)

#: `CREATE UNIQUE INDEX ux_catalog_active ON catalog_versions (schema_name) WHERE
#: is_active = true` — é quem garante, no próprio banco, uma única versão ativa por
#: schema; o repositório também respeita isso (desativa antes de inserir), mas o
#: índice é a defesa que não depende de nenhum código de aplicação estar certo.
ux_catalog_active = Index(
    "ux_catalog_active",
    catalog_versions.c.schema_name,
    unique=True,
    postgresql_where=catalog_versions.c.is_active.is_(True),
)


async def create_tables(engine: AsyncEngine) -> None:
    """Cria a tabela e o índice parcial — usado por testes e pelo bootstrap (Marco 8)."""
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)


def _row_to_version(row: Row) -> CatalogVersion:
    return CatalogVersion(
        schema=decompile_schema(row.content),
        content_hash=row.content_hash,
        git_sha=row.git_sha,
        published_at=row.published_at,
        is_active=row.is_active,
        published_by=row.published_by,
    )


class PostgresCatalogRepository:
    """Leitura reconstrói o `Schema` via `decompile_schema`; escrita nunca faz
    `UPDATE` em `content` — cada publicação é uma linha nova (histórico completo)."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def get_active_version(self, schema_name: str) -> CatalogVersion | None:
        stmt = select(catalog_versions).where(
            catalog_versions.c.schema_name == schema_name,
            catalog_versions.c.is_active.is_(True),
        )
        async with self._engine.connect() as conn:
            row = (await conn.execute(stmt)).first()
        return _row_to_version(row) if row is not None else None

    async def list_active_versions(self) -> tuple[CatalogVersion, ...]:
        stmt = select(catalog_versions).where(catalog_versions.c.is_active.is_(True))
        async with self._engine.connect() as conn:
            rows = (await conn.execute(stmt)).all()
        return tuple(_row_to_version(row) for row in rows)

    async def publish_new_version(
        self,
        schema_name: str,
        content: str,
        content_hash: str,
        git_sha: str,
        published_by: str | None = None,
    ) -> CatalogVersion:
        async with self._engine.begin() as conn:
            # Desativa a anterior e insere a nova na mesma transação — nunca há um
            # instante em que duas versões do mesmo schema estejam ativas.
            await conn.execute(
                update(catalog_versions)
                .where(
                    catalog_versions.c.schema_name == schema_name,
                    catalog_versions.c.is_active.is_(True),
                )
                .values(is_active=False)
            )
            result = await conn.execute(
                insert(catalog_versions)
                .values(
                    schema_name=schema_name,
                    git_sha=git_sha,
                    content=content,
                    content_hash=content_hash,
                    published_by=published_by,
                    is_active=True,
                )
                .returning(catalog_versions)
            )
            row = result.one()
        return _row_to_version(row)
