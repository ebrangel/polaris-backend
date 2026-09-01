"""Orquestra o fluxo completo da seção 3 do contrato.

(0) limitar por cliente (Marco 9) → (1) validar contra o modelo lógico → (2) autorizar
→ (3) aplicar o teto de `limit` do schema → (4) checar o cache pelo `query_id` → (5)
resolver o dataset → (6) backpressure de fila cheia (`429`) → (7) enfileirar → (8)
aguardar o job por uma janela curta (`inline_wait_seconds`): concluiu, devolve o
resultado (`200`); não concluiu, devolve `processing` e a borda HTTP responde `202` +
poll_url.

Não há mais caminho síncrono nem estimativa de custo: toda consulta passa pela fila, e
quem executa e grava no cache é o worker (`RunQueuedQuery`). Este use case só lê o
cache.
"""

from collections.abc import Iterable
from dataclasses import replace

from application.ports.cache_gateway import CacheGateway
from application.ports.job_queue import JobQueue
from application.ports.rate_limiter import RateLimiter
from application.use_cases.resolve_dataset import ResolveDataset
from domain.errors import RateLimitedError
from domain.models import Catalog, QueryRequest, QueryResult


class ExecuteQuery:
    """Ponto de entrada único para `POST` e `GET /v1/query` (Marco 6): os dois convergem
    para o mesmo `QueryRequest` e chamam este use case.
    """

    def __init__(
        self,
        catalog: Catalog,
        resolve_dataset: ResolveDataset,
        cache: CacheGateway,
        job_queue: JobQueue,
        request_rate_limiter: RateLimiter | None = None,
        max_queue_depth: int | None = None,
        default_max_limit: int | None = None,
        inline_wait_seconds: float = 2.0,
    ) -> None:
        """`request_rate_limiter` e `max_queue_depth` (Marco 9) são opcionais — sem eles
        nenhum limite é aplicado.

        `default_max_limit` é o teto de linhas aplicado a schema que não declara
        `max_limit` no catálogo — a rede que impede um schema recém-publicado de
        executar sem `LIMIT` nenhum e materializar a tabela inteira em memória.

        `inline_wait_seconds` é quanto a API aguarda o job concluir antes de devolver
        `202` + poll_url (padrão 2s)."""
        self._catalog = catalog
        self._resolve_dataset = resolve_dataset
        self._cache = cache
        self._job_queue = job_queue
        self._request_rate_limiter = request_rate_limiter
        self._max_queue_depth = max_queue_depth
        self._default_max_limit = default_max_limit
        self._inline_wait_seconds = inline_wait_seconds

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

        cached = await self._cache.get(request.cache_key)
        if cached is not None:
            return replace(cached, meta=replace(cached.meta, cached=True))

        # Só para nomear o dataset no payload do job — `ResolveDataset` é síncrono e puro
        # (percorre um `Schema` já carregado), não faz I/O nem abre engine.
        dataset = self._resolve_dataset(schema, request)

        if (
            self._max_queue_depth is not None
            and await self._job_queue.depth() >= self._max_queue_depth
        ):
            # Backpressure global (`docs/escalabilidade.md`: "Fila cheia →
            # backpressure: 429") — vale para qualquer consulta.
            raise RateLimitedError("A fila de consultas está cheia.")

        await self._job_queue.enqueue(request, dataset.name)
        return await self._job_queue.wait_for_result(
            request.query_id, self._inline_wait_seconds
        )
