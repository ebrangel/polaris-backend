"""Implementações in-memory dos 4 ports — não são adapters de produção.

Não herdam de nenhum `Protocol` (a tipagem dos ports é estrutural, de propósito: o
Marco 2 não quer acoplar `adapters/` a `application/`). O Marco 4 reutiliza estes fakes
para testar a orquestração do use case `ExecuteQuery` sem nenhum banco real.
"""

from datetime import UTC, datetime

from application.ports.query_executor import ExecutionProfile, QueryCost
from domain.models import CatalogVersion, Dataset, QueryRequest, QueryResult, QueryStatus, Schema


class InMemoryCatalogRepository:
    """Fake do `CatalogRepository`.

    A tradução `content` (JSON compilado) → `Schema` é do adapter e só existe a partir
    do Marco 8; aqui, `register()` pré-cadastra o `Schema` que `publish_new_version`
    deve usar para o schema_name informado — o fake guarda fielmente hash, git_sha e
    quem publicou, só não reimplementa um parser de catálogo.
    """

    def __init__(self) -> None:
        self._schemas: dict[str, Schema] = {}
        self._active: dict[str, CatalogVersion] = {}
        self._history: dict[str, list[CatalogVersion]] = {}

    def register(self, schema: Schema) -> None:
        """Cadastra o `Schema` que será usado nas próximas publicações desse nome."""
        self._schemas[schema.name] = schema

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
        if schema_name not in self._schemas:
            raise KeyError(
                f"nenhum Schema registrado para '{schema_name}' — chame register() antes"
            )
        version = CatalogVersion(
            schema=self._schemas[schema_name],
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
    """Fake do `QueryExecutor`: resultado e custo programáveis, chamadas registradas."""

    def __init__(
        self,
        result: QueryResult | None = None,
        cost: QueryCost | None = None,
        *,
        raises: Exception | None = None,
    ) -> None:
        self.result = result
        self.cost = cost if cost is not None else QueryCost(score=0.0, threshold=1.0)
        self.raises = raises
        self.calls: list[tuple[Dataset, QueryRequest, tuple, ExecutionProfile]] = []

    async def execute(
        self,
        dataset: Dataset,
        request: QueryRequest,
        columns: tuple,
        profile: ExecutionProfile = ExecutionProfile.LIGHT,
    ) -> QueryResult:
        self.calls.append((dataset, request, columns, profile))
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

    async def estimate_cost(self, dataset: Dataset, request: QueryRequest) -> QueryCost:
        return self.cost


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


class InMemoryJobQueue:
    """Fake do `JobQueue`. `resolve()` simula um worker completando um job."""

    def __init__(self) -> None:
        self._jobs: dict[str, QueryResult] = {}

    async def enqueue(self, request: QueryRequest, dataset_name: str) -> QueryResult:
        result = QueryResult.processing(request.query_id)
        self._jobs[request.query_id] = result
        return result

    async def get_status(self, query_id: str) -> QueryResult | None:
        return self._jobs.get(query_id)

    async def depth(self) -> int:
        return sum(1 for job in self._jobs.values() if job.status is QueryStatus.PROCESSING)

    def resolve(self, query_id: str, result: QueryResult) -> None:
        """Auxiliar de teste: substitui o status `processing` pelo resultado final."""
        self._jobs[query_id] = result
