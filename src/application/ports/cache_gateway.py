"""Port de cache de resultados (Redis, seção 3: "`query_id` ... usado como chave de cache").

A chave é `QueryRequest.cache_key` (`<schema>:<query_id>`), montada no domínio (Marco 1)
— este port não conhece nada sobre como a chave é formada, só recebe `str`. O schema no
prefixo é o que permite `clear(schema)` invalidar um schema inteiro de uma vez.
"""

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from domain.models import QueryResult


@dataclass(frozen=True, slots=True)
class CacheStats:
    """Contadores acumulados desde o boot do processo — a taxa de acerto (Marco 9) é
    derivada disso por quem consome (`GetObservabilitySnapshot`), não guardada aqui."""

    hits: int
    misses: int


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

    async def clear(self, schema: str | None = None) -> int:
        """Remove entradas do cache em lote e devolve quantas foram removidas.

        `schema=None` limpa o cache inteiro; um `schema` limpa só as entradas cuja
        chave começa com aquele schema (ver `QueryRequest.cache_key`). Os contadores
        de acerto/erro (`stats`, Marco 9) não são afetados.
        """
        ...

    async def stats(self) -> CacheStats:
        """Contadores de acerto/erro acumulados (Marco 9, observabilidade)."""
        ...
