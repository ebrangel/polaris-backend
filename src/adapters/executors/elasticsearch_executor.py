"""`ElasticsearchQueryExecutor` — implementa `QueryExecutor` (Marco 2), traduzindo a
requisição estruturada para agregações da Query DSL via `AsyncElasticsearch`.

Um só cliente (o `elasticsearch-py` já tem pool HTTP próprio — "Elasticsearch não usa
pool de conexões relacional", `docs/escalabilidade.md`), mas dois timeouts: "o mesmo
princípio de separar timeouts curtos (síncrono) de longos (assíncrono) se aplica".
"""

import asyncio

from elasticsearch import AsyncElasticsearch

from adapters.executors.elasticsearch_dsl import build_query_body, parse_response
from application.ports.query_executor import ExecutionProfile, QueryCost
from domain.errors import QueryTimeoutError
from domain.models import Column, Dataset, IndexModel, QueryRequest, QueryResult


class ElasticsearchQueryExecutor:
    """Executa `QueryRequest` já resolvidas contra um índice único do Elasticsearch."""

    def __init__(
        self,
        client: AsyncElasticsearch,
        light_timeout_seconds: float = 5.0,
        heavy_timeout_seconds: float = 300.0,
        cost_threshold: float = 50.0,
    ) -> None:
        self._client = client
        self._light_timeout_seconds = light_timeout_seconds
        self._heavy_timeout_seconds = heavy_timeout_seconds
        self._cost_threshold = cost_threshold

    def _timeout_for(self, profile: ExecutionProfile) -> float:
        if profile is ExecutionProfile.HEAVY:
            return self._heavy_timeout_seconds
        return self._light_timeout_seconds

    async def execute(
        self,
        dataset: Dataset,
        request: QueryRequest,
        columns: tuple[Column, ...],
        profile: ExecutionProfile = ExecutionProfile.LIGHT,
    ) -> QueryResult:
        assert isinstance(dataset.model, IndexModel)
        body = build_query_body(dataset, request, columns)
        timeout_seconds = self._timeout_for(profile)

        try:
            response = await asyncio.wait_for(
                self._client.search(index=dataset.model.name, **body),
                timeout=timeout_seconds,
            )
        except TimeoutError as exc:
            raise QueryTimeoutError(
                f"A consulta ao dataset '{dataset.name}' excedeu {timeout_seconds}s."
            ) from exc

        return QueryResult.completed(
            query_id=request.query_id,
            columns=columns,
            rows=parse_response(response.body, columns),
            dataset_used=dataset.name,
            execution_ms=response.body["took"],
        )

    async def estimate_cost(self, dataset: Dataset, request: QueryRequest) -> QueryCost:
        """Heurística por contagem de campos — sem `EXPLAIN` equivalente coberto neste
        marco (o mais próximo seria `_count`/profile API do Elasticsearch, fora de
        escopo; ver `SQLAlchemyQueryExecutor.estimate_cost` para o caso Postgres real).
        """
        score = len(request.dimensions) * 10 - len(request.filters) * 5
        return QueryCost(
            score=max(score, 0),
            threshold=self._cost_threshold,
            detail="heurística por contagem de campos",
        )
