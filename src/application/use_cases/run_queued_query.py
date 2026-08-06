"""Executa uma consulta já enfileirada — o lado worker da seção 2.4.

Não é o `ExecuteQuery`: aquele consulta o cache, estima custo e enfileira se pesada de
novo — chamá-lo aqui criaria um laço. O dataset já foi escolhido por `ResolveDataset`
no momento do `enqueue` (seu nome viajou no payload do job), então este use case nem o
`ResolveDataset` chama — só busca o dataset pelo nome e executa no perfil `HEAVY`.
"""

from collections.abc import Mapping

from application.ports.query_executor import ExecutionProfile, QueryExecutor
from application.use_cases._executor_lookup import executor_for
from application.use_cases._slow_query_log import log_if_slow
from domain.models import Catalog, QueryRequest, QueryResult


class RunQueuedQuery:
    """Chamado pelo worker (`adapters/queue/tasks.py`) para cada job que sai da fila."""

    def __init__(
        self,
        catalog: Catalog,
        executors: Mapping[str, QueryExecutor],
        slow_query_threshold_ms: int | None = None,
    ) -> None:
        self._catalog = catalog
        self._executors = executors
        self._slow_query_threshold_ms = slow_query_threshold_ms

    async def __call__(self, request: QueryRequest, dataset_name: str) -> QueryResult:
        schema = self._catalog.get_schema(request.schema)
        dataset = schema.get_dataset(dataset_name)
        columns = schema.columns_for(request)
        executor = executor_for(self._executors, dataset)

        result = await executor.execute(
            dataset, request, columns, profile=ExecutionProfile.HEAVY
        )
        log_if_slow(
            result, schema_name=schema.name, threshold_ms=self._slow_query_threshold_ms
        )
        return result
