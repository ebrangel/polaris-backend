"""`RedisCacheGateway` — implementa `CacheGateway` (Marco 2) sobre `redis.asyncio`.

A chave é sempre `QueryRequest.query_id` (seção 3); aqui só se soma um prefixo, para não
colidir com o namespace `arq:*` que a fila (`ArqJobQueue`) usa no mesmo Redis.
"""

import json

from redis.asyncio import Redis

from adapters.serialization import dict_to_result, result_to_dict
from domain.models import QueryResult, QueryStatus


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
            return None
        return dict_to_result(json.loads(raw))

    async def set(self, key: str, result: QueryResult, ttl_seconds: int | None = None) -> None:
        if result.status is not QueryStatus.COMPLETED:
            raise ValueError("só resultados com status=completed são cacheáveis")

        ttl = ttl_seconds if ttl_seconds is not None else self._default_ttl_seconds
        payload = json.dumps(result_to_dict(result))
        await self._client.set(self._redis_key(key), payload, ex=ttl)

    async def delete(self, key: str) -> None:
        await self._client.delete(self._redis_key(key))
