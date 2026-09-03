"""Port de cache de resultados (Redis, seção 3: "`query_id` ... usado como chave de cache").

A chave é `QueryRequest.cache_key` (`<schema>:<query_id>`), montada no domínio (Marco 1)
— este port não conhece nada sobre como a chave é formada, só recebe `str`. O schema no
prefixo é o que permite `clear(schema)` invalidar um schema inteiro de uma vez.
"""

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from application.ports.row_sink import RowSink
from domain.models import Column, QueryResult


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

    async def open_writer(
        self,
        key: str,
        columns: tuple[Column, ...],
        query_id: str,
        dataset_used: str,
        ttl_seconds: int | None = None,
    ) -> RowSink:
        """Abre um destino de escrita para o resultado desta chave.

        Substitui o antigo `set(key, result)`: gravar exigia o `QueryResult` inteiro em
        memória, que é justamente o que o Marco 12 eliminou. O sink acumula os blocos e
        só materializa a entrada no `close`, respeitando os tetos do adapter.

        **Passar do teto não é erro.** O sink desiste em silêncio (com log) e a consulta
        seguinte simplesmente executa de novo — o port promete gravar o que couber. Só
        resultados concluídos chegam aqui; quem chama garante isso.

        `query_id` e `dataset_used` entram porque a entrada de cache guarda o
        `QueryResult` inteiro da seção 2.3, e não só as linhas.
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
