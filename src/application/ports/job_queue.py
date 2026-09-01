"""Port da fila de consultas (seção 2.4 + `docs/escalabilidade.md`).

Toda consulta passa pela fila: o endpoint de submissão enfileira e aguarda o resultado
por uma janela curta (`wait_for_result`); se não concluir no tempo, devolve 202 +
poll_url e o cliente acompanha por `get_status`.
"""

from typing import Protocol, runtime_checkable

from domain.models import QueryRequest, QueryResult


@runtime_checkable
class JobQueue(Protocol):
    """Enfileira consultas e acompanha seu status por `query_id`."""

    async def enqueue(self, request: QueryRequest, dataset_name: str) -> QueryResult:
        """Enfileira a consulta e devolve o corpo do `202 Accepted` da seção 2.4.

        O resultado devolvido tem sempre `status=processing`
        (`QueryResult.processing(request.query_id)`). Recebe o **nome** do dataset já
        resolvido, não o objeto `Dataset` — o payload enfileirado precisa ser
        serializável para o broker.
        """
        ...

    async def wait_for_result(self, query_id: str, timeout: float) -> QueryResult:
        """Aguarda o job concluir por até `timeout` segundos.

        - devolve o `QueryResult` final (`status=completed`) se concluiu com sucesso no
          tempo;
        - devolve `QueryResult.failed(query_id, error=...)` se o job levantou exceção;
        - devolve `QueryResult.processing(query_id)` se o tempo esgotou, ou se o job não
          é (mais) rastreado pela fila.

        Nunca levanta por timeout nem por job ausente — quem chama transforma
        `processing` no 202 + poll_url e deixa o `GET /v1/query/{query_id}` revelar a
        realidade.
        """
        ...

    async def get_status(self, query_id: str) -> QueryResult | None:
        """Status atual do job, ou `None` se o `query_id` não existir na fila."""
        ...

    async def depth(self) -> int:
        """Número de jobs pendentes — usado para backpressure (`429`) e observabilidade."""
        ...
