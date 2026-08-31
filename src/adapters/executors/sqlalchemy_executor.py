"""`SQLAlchemyQueryExecutor` — implementa `QueryExecutor` (Marco 2) para Postgres e
Oracle via SQLAlchemy Core assíncrono.

Dois `AsyncEngine`, sem default: `docs/escalabilidade.md` é explícito — "nunca
compartilhar o mesmo pool ... entre perfis leve/pesado" — e um default silencioso
violaria isso sem avisar. O caminho síncrono da API usa `light_engine`; o worker da
fila (`RunQueuedQuery`, Marco 7) usa `heavy_engine`.
"""

import asyncio
import time
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncEngine

from adapters.executors.sql_builder import build_select
from application.ports.query_executor import ExecutionProfile, QueryCost
from domain.errors import QueryTimeoutError
from domain.models import Column, DataType, Dataset, QueryRequest, QueryResult


class SQLAlchemyQueryExecutor:
    """Executa `QueryRequest` já resolvidas contra Postgres/Oracle via SQLAlchemy Core."""

    def __init__(
        self,
        light_engine: AsyncEngine,
        heavy_engine: AsyncEngine,
        light_timeout_seconds: float = 5.0,
        heavy_timeout_seconds: float = 300.0,
        cost_threshold: float = 10_000.0,
        heuristic_threshold: float = 30.0,
    ) -> None:
        """`cost_threshold` e `heuristic_threshold` medem coisas em unidades diferentes,
        e por isso são dois números.

        O primeiro é comparado com o custo do otimizador (`EXPLAIN`), que vai à casa dos
        milhares numa varredura grande. O segundo é comparado com o score da heurística
        de fallback, que é uma contagem de campos e não passa de algumas dezenas — usar
        o limiar do `EXPLAIN` para julgá-lo tornava a heurística incapaz de classificar
        qualquer consulta como pesada, e todo datasource sem `EXPLAIN` acabava no
        caminho síncrono, dentro do processo da API.
        """
        self._light_engine = light_engine
        self._heavy_engine = heavy_engine
        self._light_timeout_seconds = light_timeout_seconds
        self._heavy_timeout_seconds = heavy_timeout_seconds
        self._cost_threshold = cost_threshold
        self._heuristic_threshold = heuristic_threshold

    def _engine_and_timeout(self, profile: ExecutionProfile) -> tuple[AsyncEngine, float]:
        if profile is ExecutionProfile.HEAVY:
            return self._heavy_engine, self._heavy_timeout_seconds
        return self._light_engine, self._light_timeout_seconds

    async def execute(
        self,
        dataset: Dataset,
        request: QueryRequest,
        columns: tuple[Column, ...],
        profile: ExecutionProfile = ExecutionProfile.LIGHT,
    ) -> QueryResult:
        stmt = build_select(dataset, request, columns)
        engine, timeout_seconds = self._engine_and_timeout(profile)

        started = time.perf_counter()
        try:
            async with engine.connect() as conn:
                result = await asyncio.wait_for(conn.execute(stmt), timeout=timeout_seconds)
                rows = result.all()
        except TimeoutError as exc:
            raise QueryTimeoutError(
                f"A consulta ao dataset '{dataset.name}' excedeu {timeout_seconds}s."
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
        """Custo do otimizador quando o dialeto tem como dá-lo — `EXPLAIN (FORMAT JSON)`
        no Postgres, `EXPLAIN PLAN`/`PLAN_TABLE` no Oracle — e heurística por contagem
        de campos em qualquer outro caso: dialeto sem caminho coberto, erro ao rodar o
        `EXPLAIN`, ou formato de plano inesperado. Estimar custo nunca pode derrubar uma
        consulta que funcionaria, por isso a captura é ampla e sempre cai para um
        resultado válido.
        """
        dialect = self._light_engine.dialect.name
        if dialect == "postgresql":
            explained = await self._explain_postgres(dataset, request)
        elif dialect == "oracle":
            explained = await self._explain_oracle(dataset, request)
        else:
            explained = None
        if explained is not None:
            return explained

        score = len(request.dimensions) * 10 - len(request.filters) * 5
        return QueryCost(
            score=max(score, 0),
            threshold=self._heuristic_threshold,
            detail="heurística por contagem de campos (EXPLAIN indisponível)",
        )

    def _compiled_sql(self, dataset: Dataset, request: QueryRequest) -> str:
        """SQL do dialeto com os valores embutidos — só para o `EXPLAIN`.

        `literal_binds` aqui não abre caminho para injeção: os valores vêm de
        `QueryRequest`, já validado contra o catálogo, e são renderizados pelo próprio
        dialeto (o mesmo escape do driver). O caminho de execução de verdade continua
        parametrizado; este SQL nunca devolve linha, só plano.
        """
        columns = _probe_columns(dataset, request)
        stmt = build_select(dataset, request, columns)
        return str(
            stmt.compile(
                dialect=self._light_engine.dialect,
                compile_kwargs={"literal_binds": True},
            )
        )

    async def _explain_postgres(
        self, dataset: Dataset, request: QueryRequest
    ) -> QueryCost | None:
        try:
            sql = self._compiled_sql(dataset, request)

            async with self._light_engine.connect() as conn:
                result = await asyncio.wait_for(
                    conn.exec_driver_sql(f"EXPLAIN (FORMAT JSON) {sql}"),
                    timeout=self._light_timeout_seconds,
                )
                plan = result.scalar_one()

            total_cost = float(plan[0]["Plan"]["Total Cost"])
        except Exception:
            # Inclui timeout do EXPLAIN, SQL sem suporte a literal_binds, permissão,
            # formato de plano inesperado — tudo cai na heurística, silenciosamente.
            return None

        return QueryCost(
            score=total_cost,
            threshold=self._cost_threshold,
            detail=f"EXPLAIN (FORMAT JSON): Total Cost = {total_cost}",
        )

    async def _explain_oracle(
        self, dataset: Dataset, request: QueryRequest
    ) -> QueryCost | None:
        """`EXPLAIN PLAN` grava o plano na `PLAN_TABLE`; a linha `ID = 0` é a raiz, com
        o custo e a cardinalidade totais da consulta.

        São dois comandos, e eles **precisam** rodar na mesma conexão: a `PLAN_TABLE` é
        uma tabela temporária de sessão, então o plano gravado por uma conexão do pool
        não existe para as outras. O `STATEMENT_ID` é gerado aqui (hex de UUID, nunca
        entrada do cliente) para que estimativas concorrentes na mesma sessão não leiam
        o plano uma da outra, e as linhas são apagadas em seguida — sem isso elas se
        acumulariam por toda a vida da conexão.
        """
        statement_id = f"polaris_{uuid4().hex}"[:30]  # PLAN_TABLE.STATEMENT_ID: 30 chars
        try:
            sql = self._compiled_sql(dataset, request)

            async with self._light_engine.connect() as conn:
                await asyncio.wait_for(
                    conn.exec_driver_sql(
                        f"EXPLAIN PLAN SET STATEMENT_ID = '{statement_id}' FOR {sql}"
                    ),
                    timeout=self._light_timeout_seconds,
                )
                result = await asyncio.wait_for(
                    conn.exec_driver_sql(
                        "SELECT COST, CARDINALITY FROM PLAN_TABLE "
                        f"WHERE STATEMENT_ID = '{statement_id}' AND ID = 0"
                    ),
                    timeout=self._light_timeout_seconds,
                )
                row = result.first()
                await conn.exec_driver_sql(
                    f"DELETE FROM PLAN_TABLE WHERE STATEMENT_ID = '{statement_id}'"
                )

            if row is None or row[0] is None:
                # Sem estatísticas no dicionário, o otimizador pode não estimar custo —
                # aí a heurística é mais honesta que um zero que passaria por "leve".
                return None
            total_cost = float(row[0])
            cardinality = row[1]
        except Exception:
            # Mesma captura ampla do Postgres: falta de `PLAN_TABLE`, permissão, timeout.
            return None

        return QueryCost(
            score=total_cost,
            threshold=self._cost_threshold,
            detail=f"EXPLAIN PLAN: Cost = {total_cost}, Cardinality = {cardinality}",
        )


def _probe_columns(dataset: Dataset, request: QueryRequest) -> tuple[Column, ...]:
    """`estimate_cost` não recebe `columns` (o port só passa `dataset`/`request`) —
    mas `build_select` precisa de uma projeção para montar o `Select`. Para o `EXPLAIN`,
    o que é projetado não importa (só o plano é lido, nunca uma linha), então usa os
    próprios nomes de `request` como `Column`s "cegas" de tipo `string`.
    """
    return tuple(
        Column(field=name, type=DataType.STRING)
        for name in (*request.dimensions, *request.measures)
    )
