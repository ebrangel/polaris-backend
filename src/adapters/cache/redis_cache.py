"""`RedisCacheGateway` — implementa `CacheGateway` (Marco 2) sobre `redis.asyncio`.

A chave é `QueryRequest.cache_key` (`<schema>:<query_id>`); aqui só se soma o prefixo
`query:`, tanto para não colidir com o namespace `arq:*` da fila (`ArqJobQueue`) e o
`ratelimit:request:*` do rate limiter no mesmo Redis, quanto para que `clear(schema)`
seja um `SCAN MATCH query:<schema>:*`.
"""

import json
import logging

from redis.asyncio import Redis

from adapters.serialization import dict_to_result, result_to_dict
from application.ports.cache_gateway import CacheStats
from domain.models import QueryResult, QueryStatus

#: Chaves fixas, sem TTL — contadores acumulados desde o boot do processo (Marco 9),
#: não por janela de tempo (isso é o rate limiter, não a taxa de acerto de cache).
_HITS_KEY = "cache:hits"
_MISSES_KEY = "cache:misses"

logger = logging.getLogger(__name__)


class RedisCacheGateway:
    """Cache de `QueryResult` por `query_id`, com TTL padrão configurável."""

    def __init__(
        self,
        client: Redis,
        key_prefix: str = "query:",
        default_ttl_seconds: int | None = 3600,
        max_rows: int | None = 100_000,
        max_payload_bytes: int | None = 8 * 1024 * 1024,
    ) -> None:
        """`max_rows`/`max_payload_bytes` são o teto do que vale a pena cachear.

        Não é uma decisão de negócio (por isso mora aqui, e não no use case): é a
        realidade do Redis, que guarda cada valor inteiro em memória e recusa valores
        acima do seu próprio limite. Um resultado gigante gravado a cada consulta
        expulsaria do cache todas as consultas pequenas — que são justamente as que se
        repetem e as que o cache existe para acelerar.

        Os dois tetos são checados em momentos diferentes de propósito: `max_rows`
        **antes** de serializar (é o que evita montar um JSON de centenas de MB só para
        descobrir que ele não cabe) e `max_payload_bytes` depois, sobre o tamanho real.
        `None` em qualquer um deles desliga aquele teto.
        """
        self._client = client
        self._key_prefix = key_prefix
        self._default_ttl_seconds = default_ttl_seconds
        self._max_rows = max_rows
        self._max_payload_bytes = max_payload_bytes

    def _redis_key(self, key: str) -> str:
        return f"{self._key_prefix}{key}"

    async def get(self, key: str) -> QueryResult | None:
        raw = await self._client.get(self._redis_key(key))
        if raw is None:
            await self._client.incr(_MISSES_KEY)
            return None
        await self._client.incr(_HITS_KEY)
        return dict_to_result(json.loads(raw))

    async def set(self, key: str, result: QueryResult, ttl_seconds: int | None = None) -> None:
        """Resultado acima dos tetos não é cacheado — e isso **não** é erro.

        O port promete gravar o que couber; um resultado grande demais é ignorado com
        log, e a consulta seguinte simplesmente executa de novo. Levantar exceção aqui
        transformaria um limite de operação em falha de requisição.
        """
        if result.status is not QueryStatus.COMPLETED:
            raise ValueError("só resultados com status=completed são cacheáveis")

        if self._max_rows is not None and len(result.rows) > self._max_rows:
            logger.info(
                "resultado de %s não cacheado: %d linhas acima do teto de %d",
                key, len(result.rows), self._max_rows,
            )
            return

        payload = json.dumps(result_to_dict(result))
        # `json.dumps` escapa não-ASCII por padrão, então o payload é ASCII puro e
        # `len()` (caracteres) é exatamente o tamanho em bytes — sem pagar uma cópia
        # extra do texto inteiro só para medi-lo com `encode()`.
        if self._max_payload_bytes is not None and len(payload) > self._max_payload_bytes:
            logger.info(
                "resultado de %s não cacheado: %d bytes acima do teto de %d",
                key, len(payload), self._max_payload_bytes,
            )
            return

        ttl = ttl_seconds if ttl_seconds is not None else self._default_ttl_seconds
        await self._client.set(self._redis_key(key), payload, ex=ttl)

    async def delete(self, key: str) -> None:
        await self._client.delete(self._redis_key(key))

    async def clear(self, schema: str | None = None) -> int:
        """Varre por padrão e apaga em lotes — não há registro de chaves para consultar.

        `cache:hits`/`cache:misses` não têm o prefixo `query:`, então nunca casam; o
        mesmo vale para `arq:*` e `ratelimit:request:*` no mesmo Redis.
        """
        match = (
            f"{self._key_prefix}{schema}:*"
            if schema is not None
            else f"{self._key_prefix}*"
        )
        removed = 0
        batch: list[str] = []
        async for redis_key in self._client.scan_iter(match=match, count=500):
            batch.append(redis_key)
            if len(batch) >= 500:
                removed += await self._client.delete(*batch)
                batch.clear()
        if batch:
            removed += await self._client.delete(*batch)
        return removed

    async def stats(self) -> CacheStats:
        hits, misses = await self._client.mget(_HITS_KEY, _MISSES_KEY)
        return CacheStats(hits=int(hits or 0), misses=int(misses or 0))
