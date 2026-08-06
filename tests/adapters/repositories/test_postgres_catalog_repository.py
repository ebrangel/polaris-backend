"""`PostgresCatalogRepository` contra um Postgres real (testcontainers) — a DDL de
`docs/pipeline-publicacao.md` aplicada de verdade, incluindo o índice parcial que
garante uma única versão ativa por schema.
"""

import shutil
import subprocess

import pytest
from fixtures import estoque_schema, vendas_schema
from sqlalchemy import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.community.postgres import PostgresContainer

from adapters.repositories.postgres_catalog_repository import (
    PostgresCatalogRepository,
    catalog_versions,
    create_tables,
)
from application.catalog_codec import canonical_json, content_hash, schema_to_dict

pytestmark = pytest.mark.integration


def _docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        subprocess.run(["docker", "info"], check=True, capture_output=True, timeout=5)
        return True
    except Exception:
        return False


if not _docker_available():
    pytest.skip("Docker indisponível — pulando testes de integração", allow_module_level=True)


async def _apply_ddl(url: str) -> None:
    engine = create_async_engine(url)
    await create_tables(engine)
    await engine.dispose()


@pytest.fixture(scope="module")
def pg_url():
    import asyncio

    with PostgresContainer("postgres:16-alpine", driver="psycopg") as pg:
        url = pg.get_connection_url()
        asyncio.run(_apply_ddl(url))
        yield url


@pytest.fixture
async def pg_engine(pg_url):
    engine = create_async_engine(pg_url)
    yield engine
    await engine.dispose()


@pytest.fixture
def repository(pg_engine) -> PostgresCatalogRepository:
    return PostgresCatalogRepository(pg_engine)


def _vendas_content() -> tuple[str, str]:
    content = canonical_json(schema_to_dict(vendas_schema()))
    return content, content_hash(content)


def _estoque_content() -> tuple[str, str]:
    content = canonical_json(schema_to_dict(estoque_schema()))
    return content, content_hash(content)


async def test_get_active_version_de_schema_desconhecido_e_none(repository):
    assert await repository.get_active_version("inexistente") is None


async def test_publish_new_version_grava_e_reconstroi_o_schema_identico(repository):
    content, hash_ = _vendas_content()

    version = await repository.publish_new_version(
        schema_name="vendas",
        content=content,
        content_hash=hash_,
        git_sha="abc123",
        published_by="pipeline-ci",
    )

    assert version.schema == vendas_schema()
    assert version.is_active
    assert version.content_hash == hash_
    assert version.git_sha == "abc123"
    assert version.published_by == "pipeline-ci"


async def test_get_active_version_devolve_a_versao_publicada(repository):
    content, hash_ = _estoque_content()
    published = await repository.publish_new_version("estoque", content, hash_, "sha1")

    active = await repository.get_active_version("estoque")

    assert active == published
    assert active.schema == estoque_schema()


async def test_publicar_duas_versoes_mantem_so_a_ultima_ativa(repository):
    content, hash_ = _estoque_content()
    first = await repository.publish_new_version("estoque", content, hash_, "sha1")

    # Conteúdo diferente (hash diferente) — segunda versão de verdade, não duplicata.
    content_v2 = canonical_json({**schema_to_dict(estoque_schema()), "description": "v2"})
    second = await repository.publish_new_version(
        "estoque", content_v2, content_hash(content_v2), "sha2"
    )

    active = await repository.get_active_version("estoque")
    assert active == second
    assert active.content_hash != first.content_hash


async def test_list_active_versions_popula_um_catalog_completo(repository):
    vendas_content, vendas_hash = _vendas_content()
    estoque_content, estoque_hash = _estoque_content()
    await repository.publish_new_version("vendas", vendas_content, vendas_hash, "sha1")
    await repository.publish_new_version("estoque", estoque_content, estoque_hash, "sha2")

    versions = await repository.list_active_versions()

    schemas = {version.schema.name: version.schema for version in versions}
    assert schemas["vendas"] == vendas_schema()
    assert schemas["estoque"] == estoque_schema()


async def test_indice_parcial_impede_duas_versoes_ativas_do_mesmo_schema(pg_engine):
    """Mesmo contornando o repositório (INSERT direto, sem o UPDATE que desativa a
    anterior), o banco recusa — a defesa não depende do código da aplicação estar
    certo (`ux_catalog_active`, `docs/pipeline-publicacao.md`)."""
    content, hash_ = _estoque_content()

    async with pg_engine.begin() as conn:
        await conn.execute(
            insert(catalog_versions).values(
                schema_name="estoque_duplicado",
                git_sha="sha1",
                content=content,
                content_hash=hash_,
                is_active=True,
            )
        )

    with pytest.raises(IntegrityError):
        async with pg_engine.begin() as conn:
            await conn.execute(
                insert(catalog_versions).values(
                    schema_name="estoque_duplicado",
                    git_sha="sha2",
                    content=content,
                    content_hash=hash_,
                    is_active=True,
                )
            )
