"""`PurgeCache` — limpeza forçada do cache de resultados, tudo ou por schema.

Só orquestra o port `CacheGateway.clear`; a decisão de "o que é uma entrada do schema
X" mora na chave (`QueryRequest.cache_key`) e no adapter que a varre, não aqui. Não
recebe o catálogo: pedir a limpeza de um schema inexistente é um no-op (nada casa o
scan), não um erro.
"""

from application.ports.cache_gateway import CacheGateway


class PurgeCache:
    def __init__(self, cache: CacheGateway) -> None:
        self._cache = cache

    async def __call__(self, *, schema: str | None = None) -> int:
        """Devolve quantas entradas foram removidas. `schema=None` limpa tudo."""
        return await self._cache.clear(schema)
