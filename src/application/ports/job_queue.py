"""Port da fila de consultas pesadas (seção 2.4 + `docs/escalabilidade.md`)."""

from typing import Protocol, runtime_checkable

from domain.models import QueryRequest, QueryResult


@runtime_checkable
class JobQueue(Protocol):
    """Enfileira consultas pesadas e acompanha seu status por `query_id`."""

    async def enqueue(self, request: QueryRequest, dataset_name: str) -> QueryResult:
        """Enfileira a consulta e devolve o corpo do `202 Accepted` da seção 2.4.

        O resultado devolvido tem sempre `status=processing`
        (`QueryResult.processing(request.query_id)`). Recebe o **nome** do dataset já
        resolvido, não o objeto `Dataset` — o payload enfileirado precisa ser
        serializável para o broker.
        """
        ...

    async def get_status(self, query_id: str) -> QueryResult | None:
        """Status atual do job, ou `None` se o `query_id` não existir na fila."""
        ...

    async def depth(self) -> int:
        """Número de jobs pendentes — usado para backpressure (`429`) e observabilidade."""
        ...
