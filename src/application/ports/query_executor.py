"""Port de execução de consultas contra um dataset.

Uma implementação por família de datasource: `SQLAlchemyQueryExecutor` (Postgres,
Oracle) e `ElasticsearchQueryExecutor` (Marco 5) — ambas atendem este mesmo contrato,
para que o use case `RunQueuedQuery` (worker) não precise saber qual delas está usando.
"""

from typing import Protocol, runtime_checkable

from domain.models import Column, Dataset, QueryRequest, QueryResult


@runtime_checkable
class QueryExecutor(Protocol):
    """Executa uma `QueryRequest` já resolvida para um `Dataset` específico."""

    async def execute(
        self,
        dataset: Dataset,
        request: QueryRequest,
        columns: tuple[Column, ...],
    ) -> QueryResult:
        """Executa a consulta e devolve um `QueryResult` com `status=completed`.

        `columns` vem de `Schema.columns_for(request)` — o executor usa os tipos e o
        `format` de cada coluna para montar a resposta da seção 2.3, sem precisar
        conhecer o `Schema` inteiro. Levanta `domain.errors.QueryTimeoutError` se a
        consulta estourar o timeout configurado no executor.
        """
        ...
