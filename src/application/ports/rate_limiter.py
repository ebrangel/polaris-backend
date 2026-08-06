"""Port de limitação de requisições por cliente (`docs/escalabilidade.md`: "Rate
limiting por cliente" — limite de requisições por chave de API + limite mais
restritivo de consultas pesadas em fila).

Uma única instância cobre um único limite (janela e teto próprios); o composition
root monta uma instância por limite — mesmo padrão de `executors`, um port com várias
instâncias configuradas de formas diferentes.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class RateLimiter(Protocol):
    """Limita quantas vezes um `client_id` pode passar por um ponto de controle."""

    async def allow(self, client_id: str) -> bool:
        """`True` se o cliente ainda está dentro do limite — e o consome; `False` se
        o limite já foi atingido (a chamada não conta contra o próximo período)."""
        ...
