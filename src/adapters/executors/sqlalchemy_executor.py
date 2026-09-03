"""`SQLAlchemyQueryExecutor` — implementa `QueryExecutor` (Marco 2) para Postgres e
Oracle via SQLAlchemy Core assíncrono.

Um `AsyncEngine` por datasource, sem default (`docs/escalabilidade.md`): quem constrói
o executor é o composition root, que já tem a URL resolvida. Todas as consultas rodam
pelo worker (`RunQueuedQuery`), com um único timeout por datasource.

**Leitura em blocos (Marco 12).** O cursor é lido com `conn.stream()` +
`AsyncResult.partitions(chunk_size)`, e cada bloco vai direto para o `RowSink` — nunca
existe uma cópia do resultado inteiro em memória. É por isso que o port é push: um
`AsyncResult` só vale enquanto a conexão que o produziu está aberta, e ela fecha no fim
do `async with` aqui dentro; devolver o cursor ao chamador entregaria um objeto morto.
"""

import asyncio
import time
from collections.abc import Sequence
from typing import Any

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from adapters.executors.sql_builder import build_count, build_select, needs_window_count
from application.ports.row_sink import RowSink, StreamedResult
from domain.errors import QueryTimeoutError
from domain.models import Column, Dataset, QueryRequest

#: Linhas lidas do cursor por vez. Não é o `LIMIT` da consulta — é o tamanho do lote que
#: viaja do banco para o worker de cada vez, e portanto o teto de memória do laço.
DEFAULT_CHUNK_SIZE = 1000


class SQLAlchemyQueryExecutor:
    """Executa `QueryRequest` já resolvidas contra Postgres/Oracle via SQLAlchemy Core."""

    def __init__(
        self,
        engine: AsyncEngine,
        timeout_seconds: float = 300.0,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> None:
        if chunk_size <= 0:
            raise ValueError(f"`chunk_size` precisa ser positivo: {chunk_size}.")
        self._engine = engine
        self._timeout_seconds = timeout_seconds
        self._chunk_size = chunk_size

    async def execute(
        self,
        dataset: Dataset,
        request: QueryRequest,
        columns: tuple[Column, ...],
        sink: RowSink,
    ) -> StreamedResult:
        started = time.perf_counter()
        try:
            # O timeout envolve a leitura inteira, e não só o envio da consulta: com
            # cursor em blocos, é o laço que domina o tempo. (Antes do Marco 12 o
            # `result.all()` ficava de fora do `wait_for`, e o timeout mal cobria o
            # trecho mais barato da operação.)
            row_count, total_rows = await asyncio.wait_for(
                self._drain(dataset, request, columns, sink),
                timeout=self._timeout_seconds,
            )
        except (TimeoutError, asyncio.TimeoutError) as exc:
            raise QueryTimeoutError(
                f"A consulta ao dataset '{dataset.name}' excedeu {self._timeout_seconds}s."
            ) from exc

        return StreamedResult(
            row_count=row_count,
            total_rows=total_rows,
            execution_ms=round((time.perf_counter() - started) * 1000),
        )

    async def _drain(
        self,
        dataset: Dataset,
        request: QueryRequest,
        columns: tuple[Column, ...],
        sink: RowSink,
    ) -> tuple[int, int | None]:
        """Lê o cursor em blocos, empurra cada um para o sink e apura os contadores."""
        stmt = build_select(dataset, request, columns)
        windowed = needs_window_count(request)
        width = len(columns)

        row_count = 0
        total_rows: int | None = None

        async with self._engine.connect() as conn:
            result = await conn.stream(stmt)
            async for partition in result.partitions(self._chunk_size):
                if windowed and total_rows is None and partition:
                    # A janela repete o mesmo total em toda linha; ler da primeira já
                    # basta, e é o que faz o total chegar junto do primeiro bloco em vez
                    # de no fim.
                    total_rows = partition[0][-1]
                rows = self._strip(partition, width)
                row_count += len(rows)
                await sink.write(rows)

            if not windowed:
                total_rows = await self._total_without_window(
                    conn, dataset, request, columns, row_count
                )
            elif total_rows is None:
                # `offset` além do fim: zero linhas, portanto nenhuma janela para ler.
                # O total existe e não é zero — precisa vir da contagem de apoio.
                total_rows = await self._count(conn, dataset, request, columns)

        return row_count, total_rows

    @staticmethod
    def _strip(
        partition: Sequence[Any], width: int
    ) -> list[tuple[Any, ...]]:
        """Linha do driver → tupla com exatamente `width` valores.

        O fatiamento retira a coluna auxiliar da janela quando ela está presente; sem
        janela, `row[:width]` é a linha inteira e o corte não custa nada. A coluna
        auxiliar nunca chega ao sink, o que mantém válida a invariante de largura de
        `QueryResult`.
        """
        return [tuple(row)[:width] for row in partition]

    async def _total_without_window(
        self,
        conn: AsyncConnection,
        dataset: Dataset,
        request: QueryRequest,
        columns: tuple[Column, ...],
        row_count: int,
    ) -> int:
        """O total quando a consulta saiu sem coluna de janela (`offset == 0`).

        Se veio menos que o `limit`, o resultado é o próprio total e nada é perguntado ao
        banco — é o caso comum, e o que torna a política "só quando pagina" barata. Se
        bateu no `limit`, pode haver mais, e só aí sai a contagem de apoio.
        """
        if request.limit is None or row_count < request.limit:
            return row_count
        return await self._count(conn, dataset, request, columns)

    async def _count(
        self,
        conn: AsyncConnection,
        dataset: Dataset,
        request: QueryRequest,
        columns: tuple[Column, ...],
    ) -> int:
        result = await conn.execute(build_count(dataset, request, columns))
        return int(result.scalar_one())
