"""Orquestra o fluxo completo da seção 3 do contrato.

(1) validar contra o modelo lógico → (2) autorizar → (3) aplicar o teto de `limit` do
schema → (4) checar o cache pelo `query_id` → (5) resolver o dataset → (6) estimar
custo e enfileirar se pesada (seção 2.4) → (7) executar → (8) gravar no cache.
"""

from collections.abc import Iterable, Mapping
from dataclasses import replace

from application.ports.cache_gateway import CacheGateway
from application.ports.job_queue import JobQueue
from application.ports.query_executor import QueryExecutor
from application.use_cases._executor_lookup import executor_for
from application.use_cases.resolve_dataset import ResolveDataset
from domain.models import Catalog, DatasourceType, QueryRequest, QueryResult, QueryStatus


class ExecuteQuery:
    """Ponto de entrada único para `POST` e `GET /v1/query` (Marco 6): os dois convergem
    para o mesmo `QueryRequest` e chamam este use case.
    """

    def __init__(
        self,
        catalog: Catalog,
        resolve_dataset: ResolveDataset,
        executors: Mapping[DatasourceType, QueryExecutor],
        cache: CacheGateway,
        job_queue: JobQueue,
        cache_ttl_seconds: int | None = None,
    ) -> None:
        self._catalog = catalog
        self._resolve_dataset = resolve_dataset
        self._executors = executors
        self._cache = cache
        self._job_queue = job_queue
        self._cache_ttl_seconds = cache_ttl_seconds

    async def __call__(self, request: QueryRequest, *, roles: Iterable[str]) -> QueryResult:
        schema = self._catalog.get_schema(request.schema)

        schema.validate_request(request)  # passo (1) da seção 3
        schema.authorize(request, roles)  # `forbidden_measure`, antes de tocar o cache

        # Teto de `limit` por schema (seção 2.6), aplicado antes do `query_id`: duas
        # requisições que só diferem num `limit` acima do teto compartilham a mesma
        # entrada de cache, e o `query_id` devolvido corresponde ao que foi executado.
        request = replace(request, limit=schema.effective_limit(request.limit))

        cached = await self._cache.get(request.query_id)
        if cached is not None:
            return replace(cached, meta=replace(cached.meta, cached=True))

        dataset = self._resolve_dataset(schema, request)
        columns = schema.columns_for(request)
        executor = executor_for(self._executors, dataset)

        # "Depois de resolver o dataset, estimar custo ... antes de executar; acima de
        # um limiar, enfileirar" (seção 2.4). Só depois do cache: um acerto não paga o
        # custo de estimar (que pode ir ao banco — EXPLAIN, Marco 7).
        cost = await executor.estimate_cost(dataset, request)
        if cost.is_heavy:
            return await self._job_queue.enqueue(request, dataset.name)

        result = await executor.execute(dataset, request, columns)

        if result.status is QueryStatus.COMPLETED:
            await self._cache.set(request.query_id, result, self._cache_ttl_seconds)

        return result
