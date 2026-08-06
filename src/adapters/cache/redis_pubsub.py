"""`RedisCatalogInvalidator` — implementa `CatalogInvalidator` (Marco 8) via Redis
Pub/Sub, no tópico `catalog:invalidate` (`docs/pipeline-publicacao.md`).

`listen_for_invalidation` é o lado assinante: um laço que roda para sempre, chamado
como task em background pelo `lifespan` da API (Marco 8) — é o que permite trocar
`app.state.catalog` "imediatamente, em vez de depender de polling periódico".
"""

from collections.abc import Awaitable, Callable

from redis.asyncio import Redis

CHANNEL = "catalog:invalidate"


class RedisCatalogInvalidator:
    def __init__(self, client: Redis, channel: str = CHANNEL) -> None:
        self._client = client
        self._channel = channel

    async def publish(self, schema_name: str) -> None:
        await self._client.publish(self._channel, schema_name)


async def listen_for_invalidation(
    client: Redis,
    on_invalidate: Callable[[str], Awaitable[None]],
    channel: str = CHANNEL,
) -> None:
    """Assina `channel` e chama `on_invalidate(schema_name)` para cada evento.

    Roda indefinidamente (`pubsub.listen()` nunca termina sozinho) — o chamador é
    responsável por rodar isto como uma task e cancelá-la no shutdown.
    """
    pubsub = client.pubsub()
    await pubsub.subscribe(channel)
    try:
        async for message in pubsub.listen():
            if message["type"] != "message":
                continue  # a confirmação de subscribe também chega por aqui
            data = message["data"]
            schema_name = data.decode("utf-8") if isinstance(data, bytes) else data
            await on_invalidate(schema_name)
    finally:
        await pubsub.unsubscribe(channel)
        await pubsub.aclose()
