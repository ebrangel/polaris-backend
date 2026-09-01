"""Implementações in-memory dos 4 ports — não são adapters de produção.

Não herdam de nenhum `Protocol` (a tipagem dos ports é estrutural, de propósito: o
Marco 2 não quer acoplar `adapters/` a `application/`). O Marco 4 reutiliza estes fakes
para testar a orquestração do use case `ExecuteQuery` sem nenhum banco real.
"""

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

from application.catalog_codec import decompile_schema
from application.ports.cache_gateway import CacheStats
from application.ports.result_exporter import ExportMetadata
from domain.models import CatalogVersion, Dataset, QueryRequest, QueryResult, QueryStatus


class InMemoryCatalogRepository:
    """Fake do `CatalogRepository`. Desserializa `content` de verdade via
    `catalog_codec.decompile_schema` (Marco 8) — como qualquer repositório real faria
    ao ler uma linha de volta; não precisa mais de um `Schema` pré-cadastrado."""

    def __init__(self) -> None:
        self._active: dict[str, CatalogVersion] = {}
        self._history: dict[str, list[CatalogVersion]] = {}

    async def get_active_version(self, schema_name: str) -> CatalogVersion | None:
        return self._active.get(schema_name)

    async def list_active_versions(self) -> tuple[CatalogVersion, ...]:
        return tuple(self._active.values())

    async def publish_new_version(
        self,
        schema_name: str,
        content: str,
        content_hash: str,
        git_sha: str,
        published_by: str | None = None,
    ) -> CatalogVersion:
        version = CatalogVersion(
            schema=decompile_schema(content),
            content_hash=content_hash,
            git_sha=git_sha,
            published_at=datetime.now(UTC),
            is_active=True,
            published_by=published_by,
        )
        self._history.setdefault(schema_name, []).append(version)
        self._active[schema_name] = version
        return version

    def history(self, schema_name: str) -> tuple[CatalogVersion, ...]:
        """Auxiliar de teste: todas as versões já publicadas para o schema, em ordem."""
        return tuple(self._history.get(schema_name, ()))


class StubQueryExecutor:
    """Fake do `QueryExecutor`: resultado programável, chamadas registradas."""

    def __init__(
        self,
        result: QueryResult | None = None,
        *,
        raises: Exception | None = None,
    ) -> None:
        self.result = result
        self.raises = raises
        self.calls: list[tuple[Dataset, QueryRequest, tuple]] = []

    async def execute(
        self,
        dataset: Dataset,
        request: QueryRequest,
        columns: tuple,
    ) -> QueryResult:
        self.calls.append((dataset, request, columns))
        if self.raises is not None:
            raise self.raises
        if self.result is not None:
            return self.result
        return QueryResult.completed(
            query_id=request.query_id,
            columns=columns,
            rows=(),
            dataset_used=dataset.name,
        )


class InMemoryCacheGateway:
    """Fake do `CacheGateway`, com contadores de acerto/erro para os testes."""

    def __init__(self) -> None:
        self._store: dict[str, QueryResult] = {}
        self.hits = 0
        self.misses = 0

    async def get(self, key: str) -> QueryResult | None:
        result = self._store.get(key)
        if result is None:
            self.misses += 1
        else:
            self.hits += 1
        return result

    async def set(self, key: str, result: QueryResult, ttl_seconds: int | None = None) -> None:
        if result.status is not QueryStatus.COMPLETED:
            raise ValueError("só resultados com status=completed são cacheáveis")
        self._store[key] = result

    async def delete(self, key: str) -> None:
        self._store.pop(key, None)

    async def clear(self, schema: str | None = None) -> int:
        prefix = None if schema is None else f"{schema}:"
        doomed = [
            key
            for key in self._store
            if prefix is None or key.startswith(prefix)
        ]
        for key in doomed:
            del self._store[key]
        return len(doomed)

    async def stats(self) -> CacheStats:
        return CacheStats(hits=self.hits, misses=self.misses)


class InMemoryRateLimiter:
    """Fake do `RateLimiter`: teto fixo por `client_id`, contadores independentes."""

    def __init__(self, limit: int = 1_000_000) -> None:
        self._limit = limit
        self._counts: dict[str, int] = {}
        self.calls: list[str] = []

    async def allow(self, client_id: str) -> bool:
        self.calls.append(client_id)
        count = self._counts.get(client_id, 0) + 1
        self._counts[client_id] = count
        return count <= self._limit


class StubDatasourceInspector:
    """Fake do `DatasourceInspector`: campos faltantes programáveis por dataset."""

    def __init__(self, missing: dict[str, tuple[str, ...]] | None = None) -> None:
        self._missing = missing or {}
        self.calls: list[Dataset] = []

    async def missing_fields(self, dataset: Dataset) -> tuple[str, ...]:
        self.calls.append(dataset)
        return self._missing.get(dataset.name, ())


class InMemoryCatalogInvalidator:
    """Fake do `CatalogInvalidator`: registra os schemas invalidados, em ordem."""

    def __init__(self) -> None:
        self.published: list[str] = []

    async def publish(self, schema_name: str) -> None:
        self.published.append(schema_name)


class InMemoryResultExporter:
    """Fake do `ResultExporter`: os "arquivos" são bytes num dict.

    `expire()` e `raises` deixam os testes exercitarem os dois caminhos que no adapter
    real dependem de relógio e de disco — export vencido e falha de escrita.
    """

    def __init__(self, ttl_seconds: int = 86_400, *, raises: Exception | None = None) -> None:
        self._ttl = timedelta(seconds=ttl_seconds)
        self.raises = raises
        self._files: dict[str, bytes] = {}
        self._created: dict[str, datetime] = {}
        self.calls: list[str] = []
        #: `query_id`s que `open()` deve fingir que sumiram entre o `stat()` e a leitura.
        self.vanished: set[str] = set()

    def _metadata(self, query_id: str) -> ExportMetadata:
        created_at = self._created[query_id]
        return ExportMetadata(
            query_id=query_id,
            size_bytes=len(self._files[query_id]),
            created_at=created_at,
            expires_at=created_at + self._ttl,
        )

    async def export(self, result: QueryResult) -> ExportMetadata:
        self.calls.append(result.query_id)
        if self.raises is not None:
            raise self.raises
        if result.status is not QueryStatus.COMPLETED:
            raise ValueError("só resultados com status=completed são exportáveis")

        from adapters.csv_format import csv_lines  # import local: o fake não é adapter

        self._files[result.query_id] = "".join(csv_lines(result)).encode("utf-8")
        self._created[result.query_id] = datetime.now(UTC)
        return self._metadata(result.query_id)

    async def stat(self, query_id: str) -> ExportMetadata | None:
        if query_id not in self._files:
            return None
        metadata = self._metadata(query_id)
        if metadata.expires_at <= datetime.now(UTC):
            return None
        return metadata

    async def open(self, query_id: str) -> AsyncIterator[bytes]:
        if query_id in self.vanished or query_id not in self._files:
            raise FileNotFoundError(query_id)
        content = self._files[query_id]

        async def chunks() -> AsyncIterator[bytes]:
            yield content

        return chunks()

    async def purge_expired(self) -> int:
        now = datetime.now(UTC)
        expired = [
            query_id
            for query_id, created in self._created.items()
            if created + self._ttl <= now
        ]
        for query_id in expired:
            del self._files[query_id]
            del self._created[query_id]
        return len(expired)

    def expire(self, query_id: str) -> None:
        """Auxiliar de teste: envelhece o arquivo para além do TTL."""
        self._created[query_id] = datetime.now(UTC) - self._ttl - timedelta(seconds=1)


class InMemoryJobQueue:
    """Fake do `JobQueue`. `resolve()` simula um worker completando um job.

    `default_result` simula "o worker concluiu dentro da janela de espera inline":
    quando setado, `wait_for_result` devolve esse resultado em vez de `processing`,
    sem precisar chamar `resolve()` para cada `query_id`.
    """

    def __init__(self) -> None:
        self._jobs: dict[str, QueryResult] = {}
        self.calls: list[tuple[QueryRequest, str]] = []
        self.default_result: QueryResult | None = None

    async def enqueue(self, request: QueryRequest, dataset_name: str) -> QueryResult:
        self.calls.append((request, dataset_name))
        self._jobs.setdefault(request.query_id, QueryResult.processing(request.query_id))
        return QueryResult.processing(request.query_id)

    async def wait_for_result(self, query_id: str, timeout: float) -> QueryResult:
        job = self._jobs.get(query_id)
        if job is not None and job.status is not QueryStatus.PROCESSING:
            return job
        if self.default_result is not None:
            return self.default_result
        return QueryResult.processing(query_id)

    async def get_status(self, query_id: str) -> QueryResult | None:
        return self._jobs.get(query_id)

    async def depth(self) -> int:
        return sum(1 for job in self._jobs.values() if job.status is QueryStatus.PROCESSING)

    def resolve(self, query_id: str, result: QueryResult) -> None:
        """Auxiliar de teste: substitui o status `processing` pelo resultado final."""
        self._jobs[query_id] = result
