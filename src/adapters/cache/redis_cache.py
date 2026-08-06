"""`RedisCacheGateway` — implementa `CacheGateway` (Marco 2) sobre `redis.asyncio`.

A chave é sempre `QueryRequest.query_id` (seção 3); aqui só se soma um prefixo, para não
colidir com o namespace `arq:*` que a fila (`ArqJobQueue`) usa no mesmo Redis.
"""

import json

from redis.asyncio import Redis

from adapters.serialization import dict_to_result, result_to_dict
from application.ports.cache_gateway import CacheStats
from domain.models import QueryResult, QueryStatus

#: Chaves fixas, sem TTL — contadores acumulados desde o boot do processo (Marco 9),
#: não por janela de tempo (isso é o rate limiter, não a taxa de acerto de cache).
_HITS_KEY = "cache:hits"
_MISSES_KEY = "cache:misses"


class RedisCacheGateway:
    """Cache de `QueryResult` por `query_id`, com TTL padrão configurável."""

    def __init__(
        self, client: Redis, key_prefix: str = "query:", default_ttl_seconds: int | None = 3600
    ) -> None:
        self._client = client
        self._key_prefix = key_prefix
        self._default_ttl_seconds = default_ttl_seconds

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
        if result.status is not QueryStatus.COMPLETED:
            raise ValueError("só resultados com status=completed são cacheáveis")

        ttl = ttl_seconds if ttl_seconds is not None else self._default_ttl_seconds
        payload = json.dumps(result_to_dict(result))
        await self._client.set(self._redis_key(key), payload, ex=ttl)

    async def delete(self, key: str) -> None:
        await self._client.delete(self._redis_key(key))

    async def stats(self) -> CacheStats:
        hits, misses = await self._client.mget(_HITS_KEY, _MISSES_KEY)
        return CacheStats(hits=int(hits or 0), misses=int(misses or 0))
