#!/usr/bin/env python3
"""Script de CI: publica cada schema de `catalog/schemas/*.yaml`.

O laço de `docs/pipeline-publicacao.md`: "para cada arquivo ... compara hash de
conteúdo compilado contra o que já está ativo no banco" — a comparação de hash e a
decisão de pular validação/publicação vivem dentro do use case `PublishCatalog`; este
script itera os arquivos, monta os `DatasourceInspector` e reporta o resultado.

Fiação própria, **não** reaproveita `infrastructure.bootstrap.build_context`: aquele
monta engines/inspectors a partir do catálogo **já ativo** no banco — certo para a
API/worker (só executam contra o que já está publicado), errado aqui: na primeira
publicação de um schema novo (ou de um dataset com `connection_ref` novo), a versão
ativa correspondente ainda não existe. Os inspectors precisam vir dos arquivos sendo
publicados agora, não do estado anterior.

Chamado pelo pipeline de CI/CD a cada merge na branch principal — "Git nunca é lido em
tempo de execução ... o banco é atualizado apenas por um pipeline automatizado"
(mesmo documento).
"""

import asyncio
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "src"))

from adapters.cache.redis_pubsub import RedisCatalogInvalidator  # noqa: E402
from adapters.catalog.postgres_inspector import PostgresInspector  # noqa: E402
from adapters.catalog.yaml_loader import list_schema_files, load_schema_file  # noqa: E402
from adapters.repositories.postgres_catalog_repository import (  # noqa: E402
    PostgresCatalogRepository,
)
from application.catalog_codec import compile_schema  # noqa: E402
from application.use_cases import PublishCatalog  # noqa: E402
from domain.errors import DomainError  # noqa: E402
from domain.models import DatasourceType  # noqa: E402
from infrastructure import db  # noqa: E402
from infrastructure.config import load_settings  # noqa: E402
from redis.asyncio import Redis  # noqa: E402
from sqlalchemy.ext.asyncio import create_async_engine  # noqa: E402


async def main() -> int:
    settings = load_settings()
    exit_code = 0

    # Compila todos os arquivos primeiro, só para descobrir os `connection_ref`
    # referenciados — `PublishCatalog` compila de novo internamente (puro e barato,
    # sem problema repetir); arquivo que não compila entra no relatório e é excluído
    # da descoberta de connection_ref, mas não interrompe os demais.
    pending: list[tuple[Path, dict]] = []
    schemas = []
    for path in list_schema_files():
        data = load_schema_file(path)
        try:
            schema, _content, _hash = compile_schema(data)
        except DomainError as exc:
            print(f"FALHOU     {path.name}: {exc.detail}", file=sys.stderr)
            exit_code = 1
            continue
        pending.append((path, data))
        schemas.append(schema)

    relational_engines = db.build_relational_engines(
        schemas,
        light_pool_size=settings.light_pool_size,
        heavy_pool_size=settings.heavy_pool_size,
    )
    types = db.datasource_types(schemas)
    inspectors = {
        connection_ref: PostgresInspector(engines.light)
        for connection_ref, engines in relational_engines.items()
        if types.get(connection_ref) is DatasourceType.POSTGRES
    }

    catalog_engine = create_async_engine(settings.catalog_db_url)
    repository = PostgresCatalogRepository(catalog_engine)
    invalidator = RedisCatalogInvalidator(
        Redis.from_url(settings.redis_url, decode_responses=True)
    )
    publish_catalog = PublishCatalog(
        repository=repository, inspectors=inspectors, invalidator=invalidator
    )

    for path, data in pending:
        try:
            outcome = await publish_catalog(data, git_sha=settings.git_sha)
        except DomainError as exc:
            print(f"FALHOU     {path.name}: {exc.detail}", file=sys.stderr)
            exit_code = 1
            continue

        if not outcome.published:
            print(f"inalterado {outcome.schema_name} — {outcome.reason}")
            continue

        note = ""
        if outcome.uninspected_datasets:
            note = f" (sem inspeção de datasource: {', '.join(outcome.uninspected_datasets)})"
        assert outcome.version is not None
        print(f"publicado  {outcome.schema_name} @ {outcome.version.content_hash[:12]}{note}")

    await db.dispose_relational_engines(relational_engines)
    await catalog_engine.dispose()
    return exit_code


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
