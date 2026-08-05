"""Port de cache de resultados (Redis, seção 3: "`query_id` ... usado como chave de cache").

A chave é sempre `QueryRequest.query_id`, já implementado no domínio (Marco 1) — este
port não conhece nada sobre como a chave é formada, só recebe `str`.
"""

from typing import Protocol, runtime_checkable

from domain.models import QueryResult


@runtime_checkable
class CacheGateway(Protocol):
    """Cache de resultados de consulta, indexado pelo `query_id`."""

    async def get(self, key: str) -> QueryResult | None:
        """Resultado em cache para a chave, ou `None` se ausente ou expirado."""
        ...

    async def set(self, key: str, result: QueryResult, ttl_seconds: int | None = None) -> None:
        """Grava o resultado em cache.

        Só resultados com `status=completed` são cacheáveis — resultados `processing`
        ou `failed` não têm sentido como entrada de cache; quem chama este port garante
        essa condição antes de chamar `set`.
        """
        ...

    async def delete(self, key: str) -> None:
        """Remove uma entrada do cache (ex: invalidação após publicação de catálogo)."""
        ...
