"""Implementações in-memory dos 4 ports — não são adapters de produção.

Não herdam de nenhum `Protocol` (a tipagem dos ports é estrutural, de propósito: o
Marco 2 não quer acoplar `adapters/` a `application/`). O Marco 4 reutiliza estes fakes
para testar a orquestração do use case `ExecuteQuery` sem nenhum banco real.
"""

from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime, timedelta

from application.catalog_codec import decompile_schema
from application.ports.cache_gateway import CacheStats
from application.ports.result_exporter import ExportKind, ExportMetadata
from application.ports.row_sink import StreamedResult
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
    """Fake do `QueryExecutor`: linhas programáveis, chamadas registradas.

    Empurra `rows` para o sink em blocos de `chunk_size` — o bastante para os testes
    exercitarem o caminho de várias escritas sem precisar de banco.
    """

    def __init__(
        self,
        rows: Sequence[tuple] = (),
        *,
        raises: Exception | None = None,
        total_rows: int | None = None,
        execution_ms: int = 0,
        chunk_size: int = 2,
    ) -> None:
        self.rows = list(rows)
        self.raises = raises
        self.total_rows = total_rows
        self.execution_ms = execution_ms
        self.chunk_size = chunk_size
        self.calls: list[tuple[Dataset, QueryRequest, tuple]] = []

    async def execute(
        self,
        dataset: Dataset,
        request: QueryRequest,
        columns: tuple,
        sink: object,
    ) -> StreamedResult:
        self.calls.append((dataset, request, columns))
        if self.raises is not None:
            raise self.raises
        for start in range(0, len(self.rows), self.chunk_size):
            await sink.write(self.rows[start : start + self.chunk_size])
        return StreamedResult(
            row_count=len(self.rows),
            total_rows=(
                self.total_rows if self.total_rows is not None else len(self.rows)
            ),
            execution_ms=self.execution_ms,
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

    async def open_writer(
        self,
        key: str,
        columns: tuple,
        query_id: str,
        dataset_used: str,
        ttl_seconds: int | None = None,
    ) -> "_FakeCacheSink":
        return _FakeCacheSink(self._store, key, columns, query_id, dataset_used)

    async def set(self, key: str, result: QueryResult) -> None:
        """Auxiliar de teste (não faz parte do port): semeia uma entrada pronta.

        O port grava bloco a bloco; um teste que só precisa de um acerto de cache não
        deveria encenar o streaming para chegar lá.
        """
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


class CollectingRowSink:
    """Fake do `RowSink`: guarda os blocos recebidos, sem destino nenhum.

    Serve aos testes que querem inspecionar *o que* o executor empurrou — inclusive em
    quantos blocos, que é o que distingue leitura em blocos de materialização.
    """

    def __init__(self, *, raises: Exception | None = None) -> None:
        self.raises = raises
        #: Um item por chamada de `write` — preserva o recorte em blocos.
        self.chunks: list[list[tuple]] = []
        self.closed_with: StreamedResult | None = None
        self.aborted = False

    @property
    def rows(self) -> list[tuple]:
        return [row for chunk in self.chunks for row in chunk]

    async def write(self, rows: Sequence[tuple]) -> None:
        if self.raises is not None:
            raise self.raises
        self.chunks.append([tuple(row) for row in rows])

    async def close(self, result: StreamedResult) -> None:
        self.closed_with = result

    async def abort(self) -> None:
        self.aborted = True


class _FakeCacheSink:
    """Sink do `InMemoryCacheGateway`: acumula as linhas e materializa no `close`."""

    def __init__(
        self,
        store: dict[str, QueryResult],
        key: str,
        columns: tuple,
        query_id: str,
        dataset_used: str,
    ) -> None:
        self._store = store
        self._key = key
        self._columns = columns
        self._query_id = query_id
        self._dataset_used = dataset_used
        self._rows: list[tuple] = []
        self.aborted = False

    async def write(self, rows: Sequence[tuple]) -> None:
        self._rows.extend(tuple(row) for row in rows)

    async def close(self, result: StreamedResult) -> None:
        self._store[self._key] = QueryResult.completed(
            query_id=self._query_id,
            columns=self._columns,
            rows=self._rows,
            dataset_used=self._dataset_used,
            execution_ms=result.execution_ms,
            total_rows=result.total_rows,
        )

    async def abort(self) -> None:
        self.aborted = True
        self._rows = []


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
        self._files: dict[tuple[str, ExportKind], bytes] = {}
        self._results: dict[str, QueryResult] = {}
        self._created: dict[str, datetime] = {}
        self.calls: list[str] = []
        #: `query_id`s que `open()` deve fingir que sumiram entre o `stat()` e a leitura.
        self.vanished: set[str] = set()

    def _metadata(self, query_id: str, kind: ExportKind) -> ExportMetadata:
        created_at = self._created[query_id]
        return ExportMetadata(
            query_id=query_id,
            kind=kind,
            size_bytes=len(self._files[(query_id, kind)]),
            created_at=created_at,
            expires_at=created_at + self._ttl,
        )

    async def open_writer(
        self, query_id: str, columns: tuple, dataset_used: str
    ) -> "_FakeExportSink":
        self.calls.append(query_id)
        if self.raises is not None:
            raise self.raises
        return _FakeExportSink(self, query_id, columns, dataset_used)

    async def export(self, result: QueryResult) -> ExportMetadata:
        """Auxiliar de teste (não faz parte do port): grava um `QueryResult` inteiro.

        O port é alimentado bloco a bloco pelo executor; um teste que só quer um export
        pronto para exercitar download, TTL ou status não deveria ter que encenar o
        streaming. Aqui o resultado já está em memória, então empurrá-lo de uma vez é
        exatamente equivalente.
        """
        if result.status is not QueryStatus.COMPLETED:
            raise ValueError("só resultados com status=completed são exportáveis")
        assert result.meta is not None
        sink = await self.open_writer(
            result.query_id, result.columns, result.meta.dataset_used
        )
        await sink.write(list(result.rows or ()))
        await sink.close(
            StreamedResult(
                row_count=result.meta.row_count,
                total_rows=result.meta.total_rows,
                execution_ms=result.meta.execution_ms,
            )
        )
        return self._metadata(result.query_id, ExportKind.CSV)

    async def stat(
        self, query_id: str, kind: ExportKind = ExportKind.CSV
    ) -> ExportMetadata | None:
        if (query_id, kind) not in self._files:
            return None
        metadata = self._metadata(query_id, kind)
        if metadata.expires_at <= datetime.now(UTC):
            return None
        return metadata

    async def read_result(self, query_id: str) -> QueryResult | None:
        if await self.stat(query_id, ExportKind.META) is None:
            return None
        return self._results.get(query_id)

    async def open(
        self, query_id: str, kind: ExportKind = ExportKind.CSV
    ) -> AsyncIterator[bytes]:
        if query_id in self.vanished or (query_id, kind) not in self._files:
            raise FileNotFoundError(query_id)
        content = self._files[(query_id, kind)]

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
            for kind in ExportKind:
                self._files.pop((query_id, kind), None)
            self._results.pop(query_id, None)
            del self._created[query_id]
        return len(expired)

    def expire(self, query_id: str) -> None:
        """Auxiliar de teste: envelhece o arquivo para além do TTL."""
        self._created[query_id] = datetime.now(UTC) - self._ttl - timedelta(seconds=1)


class _FakeExportSink:
    """Sink do `InMemoryResultExporter`: monta CSV, JSONL e o descritor em memória."""

    def __init__(
        self,
        exporter: "InMemoryResultExporter",
        query_id: str,
        columns: tuple,
        dataset_used: str,
    ) -> None:
        self._exporter = exporter
        self._query_id = query_id
        self._columns = columns
        self._dataset_used = dataset_used
        self._rows: list[tuple] = []
        self.aborted = False

    async def write(self, rows: Sequence[tuple]) -> None:
        self._rows.extend(tuple(row) for row in rows)

    async def close(self, result: StreamedResult) -> None:
        # Imports locais: o fake não é adapter, e trazer o formatador para o topo faria
        # `tests/fakes.py` parecer parte da camada de adapters.
        import json as _json

        from adapters.csv_format import CsvRowFormatter
        from adapters.serialization import jsonable

        formatter = CsvRowFormatter()
        csv_text = formatter.header(self._columns) + "".join(
            formatter.row(row) for row in self._rows
        )
        jsonl_text = "".join(
            _json.dumps([jsonable(v) for v in row], ensure_ascii=False) + "\n"
            for row in self._rows
        )

        self._exporter._files[(self._query_id, ExportKind.CSV)] = csv_text.encode("utf-8")
        self._exporter._files[(self._query_id, ExportKind.JSONL)] = jsonl_text.encode("utf-8")
        self._exporter._files[(self._query_id, ExportKind.META)] = b"{}"
        self._exporter._results[self._query_id] = QueryResult.streamed(
            query_id=self._query_id,
            columns=self._columns,
            row_count=result.row_count,
            total_rows=result.total_rows,
            dataset_used=self._dataset_used,
            execution_ms=result.execution_ms,
        )
        self._exporter._created[self._query_id] = datetime.now(UTC)

    async def abort(self) -> None:
        self.aborted = True
        self._rows = []


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
