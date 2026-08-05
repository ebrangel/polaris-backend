"""`SQLAlchemyQueryExecutor` — implementa `QueryExecutor` (Marco 2) para Postgres e
Oracle via SQLAlchemy Core assíncrono.

Um `AsyncEngine` por instância: a separação de pools leve/pesado por `ExecutionProfile`
é trabalho do Marco 7 — aqui `profile` é aceito para satisfazer o contrato do port, mas
ainda não roteia para engines diferentes.
"""

import asyncio
import time

from sqlalchemy.ext.asyncio import AsyncEngine

from adapters.executors.sql_builder import build_select
from application.ports.query_executor import ExecutionProfile, QueryCost
from domain.errors import QueryTimeoutError
from domain.models import Column, Dataset, QueryRequest, QueryResult


class SQLAlchemyQueryExecutor:
    """Executa `QueryRequest` já resolvidas contra Postgres/Oracle via SQLAlchemy Core."""

    def __init__(self, engine: AsyncEngine, timeout_seconds: float = 5.0) -> None:
        self._engine = engine
        self._timeout_seconds = timeout_seconds

    async def execute(
        self,
        dataset: Dataset,
        request: QueryRequest,
        columns: tuple[Column, ...],
        profile: ExecutionProfile = ExecutionProfile.LIGHT,
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

    async def estimate_cost(self, dataset: Dataset, request: QueryRequest) -> QueryCost:
        """Heurística provisória: mais dimensões pedidas eleva o score, filtro reduz.

        `EXPLAIN`-based de verdade é trabalho do Marco 7 ("quando o datasource
        suportar" — `docs/escalabilidade.md`); aqui só satisfaz o contrato do port para
        que o `ExecuteQuery` já possa ser ligado a um executor real.
        """
        score = len(request.dimensions) * 10 - len(request.filters) * 5
        return QueryCost(
            score=max(score, 0),
            threshold=50,
            detail="heurística por contagem de campos — refinada no Marco 7",
        )
