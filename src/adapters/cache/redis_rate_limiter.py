"""`RedisRateLimiter` — implementa `RateLimiter` (Marco 9) como um contador de janela
fixa em Redis: `INCR` numa chave por (cliente, janela), com `EXPIRE` armado só na
primeira ocorrência da janela — não é uma checagem separada seguida de incremento
(evitaria condição de corrida entre duas requisições concorrentes do mesmo cliente).
"""

import time

from redis.asyncio import Redis


class RedisRateLimiter:
    """Um limite (janela + teto) por instância — o composition root cria uma para o
    limite geral de requisições e outra, mais restritiva, para consultas pesadas."""

    def __init__(
        self,
        client: Redis,
        *,
        limit: int,
        window_seconds: int,
        key_prefix: str = "ratelimit:",
    ) -> None:
        self._client = client
        self._limit = limit
        self._window_seconds = window_seconds
        self._key_prefix = key_prefix

    async def allow(self, client_id: str) -> bool:
        window = int(time.time() // self._window_seconds)
        key = f"{self._key_prefix}{client_id}:{window}"

        count = await self._client.incr(key)
        if count == 1:
            await self._client.expire(key, self._window_seconds)

        return count <= self._limit
