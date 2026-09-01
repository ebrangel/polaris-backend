"""`SQLAlchemyQueryExecutor` — implementa `QueryExecutor` (Marco 2) para Postgres e
Oracle via SQLAlchemy Core assíncrono.

Um `AsyncEngine` por datasource, sem default (`docs/escalabilidade.md`): quem constrói
o executor é o composition root, que já tem a URL resolvida. Todas as consultas rodam
pelo worker (`RunQueuedQuery`), com um único timeout por datasource.
"""

import asyncio
import time

from sqlalchemy.ext.asyncio import AsyncEngine

from adapters.executors.sql_builder import build_select
from domain.errors import QueryTimeoutError
from domain.models import Column, Dataset, QueryRequest, QueryResult


class SQLAlchemyQueryExecutor:
    """Executa `QueryRequest` já resolvidas contra Postgres/Oracle via SQLAlchemy Core."""

    def __init__(
        self,
        engine: AsyncEngine,
        timeout_seconds: float = 300.0,
    ) -> None:
        self._engine = engine
        self._timeout_seconds = timeout_seconds

    async def execute(
        self,
        dataset: Dataset,
        request: QueryRequest,
        columns: tuple[Column, ...],
    ) -> QueryResult:
        stmt = build_select(dataset, request, columns)

        started = time.perf_counter()
        try:
            async with self._engine.connect() as conn:
                result = await asyncio.wait_for(
                    conn.execute(stmt), timeout=self._timeout_seconds
                )
                rows = result.all()
        except TimeoutError as exc:
            raise QueryTimeoutError(
                f"A consulta ao dataset '{dataset.name}' excedeu {self._timeout_seconds}s."
            ) from exc
        execution_ms = round((time.perf_counter() - started) * 1000)

        return QueryResult.completed(
            query_id=request.query_id,
            columns=columns,
            rows=[tuple(row) for row in rows],
            dataset_used=dataset.name,
            execution_ms=execution_ms,
        )
