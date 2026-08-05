"""Port de execução de consultas contra um dataset.

Uma implementação por família de datasource: `SQLAlchemyQueryExecutor` (Postgres,
Oracle) e `ElasticsearchQueryExecutor` (Marco 5) — ambas atendem este mesmo contrato,
para que o use case `ExecuteQuery` (Marco 4) não precise saber qual delas está usando.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Protocol, runtime_checkable

from domain.models import Column, Dataset, QueryRequest, QueryResult


class ExecutionProfile(Enum):
    """Qual pool de conexão usar (`docs/escalabilidade.md`: leve/pesado por datasource).

    O caminho síncrono da API usa `LIGHT` (muitas conexões, timeout curto); os workers
    da fila usam `HEAVY` (poucas conexões, timeout longo). Nunca compartilhar pool
    entre os dois perfis.
    """

    LIGHT = "light"
    HEAVY = "heavy"


@dataclass(frozen=True, slots=True)
class QueryCost:
    """Estimativa de custo de uma consulta, específica do datasource que a produziu.

    O limiar viaja junto do score porque o critério é calculado por datasource
    (`docs/escalabilidade.md`: "o custo típico varia muito entre um Postgres pequeno e
    um Oracle DW grande") — só o executor sabe compará-los; o use case só lê `is_heavy`.
    """

    score: float
    threshold: float
    detail: str = ""

    @property
    def is_heavy(self) -> bool:
        return self.score > self.threshold


@runtime_checkable
class QueryExecutor(Protocol):
    """Executa uma `QueryRequest` já resolvida para um `Dataset` específico."""

    async def execute(
        self,
        dataset: Dataset,
        request: QueryRequest,
        columns: tuple[Column, ...],
        profile: ExecutionProfile = ExecutionProfile.LIGHT,
    ) -> QueryResult:
        """Executa a consulta e devolve um `QueryResult` com `status=completed`.

        `columns` vem de `Schema.columns_for(request)` — o executor usa os tipos e o
        `format` de cada coluna para montar a resposta da seção 2.3, sem precisar
        conhecer o `Schema` inteiro. Levanta `domain.errors.QueryTimeoutError` se a
        consulta estourar o timeout do `profile` usado.
        """
        ...

    async def estimate_cost(self, dataset: Dataset, request: QueryRequest) -> QueryCost:
        """Estima o custo da consulta antes de executá-la.

        Pode envolver I/O (ex: `EXPLAIN PLAN` ou profile prévio, "quando o datasource
        suportar" — `docs/escalabilidade.md`), por isso também é assíncrono. O use case
        `ExecuteQuery` usa o resultado para decidir entre o caminho síncrono e a fila.
        """
        ...
