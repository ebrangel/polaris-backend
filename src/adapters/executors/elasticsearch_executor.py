"""`ElasticsearchQueryExecutor` — implementa `QueryExecutor` (Marco 2), traduzindo a
requisição estruturada para agregações da Query DSL via `AsyncElasticsearch`.
"""

import asyncio

from elasticsearch import AsyncElasticsearch

from adapters.executors.elasticsearch_dsl import build_query_body, parse_response
from application.ports.query_executor import ExecutionProfile, QueryCost
from domain.errors import QueryTimeoutError
from domain.models import Column, Dataset, IndexModel, QueryRequest, QueryResult


class ElasticsearchQueryExecutor:
    """Executa `QueryRequest` já resolvidas contra um índice único do Elasticsearch."""

    def __init__(self, client: AsyncElasticsearch, timeout_seconds: float = 5.0) -> None:
        self._client = client
        self._timeout_seconds = timeout_seconds

    async def execute(
        self,
        dataset: Dataset,
        request: QueryRequest,
        columns: tuple[Column, ...],
        profile: ExecutionProfile = ExecutionProfile.LIGHT,
    ) -> QueryResult:
        assert isinstance(dataset.model, IndexModel)
        body = build_query_body(dataset, request, columns)

        try:
            response = await asyncio.wait_for(
                self._client.search(index=dataset.model.name, **body),
                timeout=self._timeout_seconds,
            )
        except TimeoutError as exc:
            raise QueryTimeoutError(
                f"A consulta ao dataset '{dataset.name}' excedeu {self._timeout_seconds}s."
            ) from exc

        return QueryResult.completed(
            query_id=request.query_id,
            columns=columns,
            rows=parse_response(response.body, columns),
            dataset_used=dataset.name,
            execution_ms=response.body["took"],
        )

    async def estimate_cost(self, dataset: Dataset, request: QueryRequest) -> QueryCost:
        """Mesma heurística provisória do `SQLAlchemyQueryExecutor` — ver ali."""
        score = len(request.dimensions) * 10 - len(request.filters) * 5
        return QueryCost(
            score=max(score, 0),
            threshold=50,
            detail="heurística por contagem de campos — refinada no Marco 7",
        )
