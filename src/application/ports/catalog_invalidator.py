"""Port de invalidação do catálogo em memória entre réplicas.

`docs/pipeline-publicacao.md`: "Ao publicar, emitir evento em um tópico Redis Pub/Sub
(`catalog:invalidate`) para que todas as instâncias da API recarreguem o catálogo em
memória imediatamente, em vez de depender de polling periódico."
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class CatalogInvalidator(Protocol):
    """Publica um aviso de que um schema foi republicado."""

    async def publish(self, schema_name: str) -> None:
        """Emite o evento de invalidação para o schema — não espera confirmação de
        que alguém assinou; quem assina é o `lifespan` da própria API (`main.py`)."""
        ...
