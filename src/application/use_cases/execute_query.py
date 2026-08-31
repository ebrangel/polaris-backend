"""Orquestra o fluxo completo da seção 3 do contrato.

(0) limitar por cliente (Marco 9) → (1) validar contra o modelo lógico → (2) autorizar
→ (3) aplicar o teto de `limit` do schema → (4) checar o cache pelo `query_id` → (5)
resolver o dataset → (6) estimar custo e enfileirar se pesada, com backpressure e
limite de cliente próprios (seção 2.4 + Marco 9) → (7) executar, logando se lenta → (8)
gravar no cache.
"""

import logging
from collections.abc import Iterable, Mapping
from dataclasses import replace

from application.ports.cache_gateway import CacheGateway
from application.ports.job_queue import JobQueue
from application.ports.query_executor import QueryExecutor
from application.ports.rate_limiter import RateLimiter
from application.use_cases._executor_lookup import executor_for
from application.use_cases._slow_query_log import log_if_slow
from application.use_cases.resolve_dataset import ResolveDataset
from domain.errors import RateLimitedError
from domain.models import Catalog, QueryRequest, QueryResult, QueryStatus

logger = logging.getLogger(__name__)


class ExecuteQuery:
    """Ponto de entrada único para `POST` e `GET /v1/query` (Marco 6): os dois convergem
    para o mesmo `QueryRequest` e chamam este use case.
    """

    def __init__(
        self,
        catalog: Catalog,
        resolve_dataset: ResolveDataset,
        executors: Mapping[str, QueryExecutor],
        cache: CacheGateway,
        job_queue: JobQueue,
        cache_ttl_seconds: int | None = None,
        request_rate_limiter: RateLimiter | None = None,
        heavy_query_rate_limiter: RateLimiter | None = None,
        max_heavy_queue_depth: int | None = None,
        slow_query_threshold_ms: int | None = None,
        default_max_limit: int | None = None,
    ) -> None:
        """Os quatro parâmetros de observabilidade/rate limiting (Marco 9) são opcionais
        — sem eles, o comportamento é idêntico ao dos Marcos 4-8: nenhum limite, nenhum
        log de consulta lenta.

        `default_max_limit` é o teto de linhas aplicado a schema que não declara
        `max_limit` no catálogo — a rede que impede um schema recém-publicado de
        executar sem `LIMIT` nenhum e materializar a tabela inteira em memória."""
        self._catalog = catalog
        self._resolve_dataset = resolve_dataset
        self._executors = executors
        self._cache = cache
        self._job_queue = job_queue
        self._cache_ttl_seconds = cache_ttl_seconds
        self._request_rate_limiter = request_rate_limiter
        self._heavy_query_rate_limiter = heavy_query_rate_limiter
        self._max_heavy_queue_depth = max_heavy_queue_depth
        self._slow_query_threshold_ms = slow_query_threshold_ms
        self._default_max_limit = default_max_limit

    async def __call__(
        self, request: QueryRequest, *, roles: Iterable[str], client_id: str
    ) -> QueryResult:
        if self._request_rate_limiter is not None:
            if not await self._request_rate_limiter.allow(client_id):
                raise RateLimitedError(
                    f"Limite de requisições excedido para o cliente '{client_id}'."
                )

        schema = self._catalog.get_schema(request.schema)

        schema.validate_request(request)  # passo (1) da seção 3
        schema.authorize(request, roles)  # `forbidden_measure`, antes de tocar o cache

        # Teto de `limit` por schema (seção 2.6), aplicado antes do `query_id`: duas
        # requisições que só diferem num `limit` acima do teto compartilham a mesma
        # entrada de cache, e o `query_id` devolvido corresponde ao que foi executado.
        request = replace(
            request, limit=schema.effective_limit(request.limit, self._default_max_limit)
        )

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
            if (
                self._max_heavy_queue_depth is not None
                and await self._job_queue.depth() >= self._max_heavy_queue_depth
            ):
                # Backpressure global (`docs/escalabilidade.md`: "Fila cheia →
                # backpressure: 429") — não é por cliente, então nem consulta o
                # `heavy_query_rate_limiter`.
                raise RateLimitedError("A fila de consultas pesadas está cheia.")
            if self._heavy_query_rate_limiter is not None:
                if not await self._heavy_query_rate_limiter.allow(client_id):
                    raise RateLimitedError(
                        f"Limite de consultas pesadas em fila excedido para o "
                        f"cliente '{client_id}'."
                    )
            return await self._job_queue.enqueue(request, dataset.name)

        result = await executor.execute(dataset, request, columns)
        log_if_slow(
            result, schema_name=schema.name, threshold_ms=self._slow_query_threshold_ms
        )

        if result.status is QueryStatus.COMPLETED:
            await self._cache_result(request.query_id, result)

        return result

    async def _cache_result(self, query_id: str, result: QueryResult) -> None:
        """Gravar no cache é otimização — nunca pode derrubar resposta já calculada.

        Mesmo raciocínio do export em `RunQueuedQuery`: o resultado existe, o cliente
        tem direito a ele. Um resultado grande demais para o Redis (valor acima do teto
        do servidor), uma indisponibilidade momentânea ou um teto do próprio adapter
        custam, no máximo, um acerto de cache na próxima requisição igual.
        """
        try:
            await self._cache.set(query_id, result, self._cache_ttl_seconds)
        except Exception:
            logger.warning(
                "falha ao gravar a consulta %s no cache", query_id, exc_info=True
            )
