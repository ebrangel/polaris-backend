"""`SQLAlchemyQueryExecutor` — roteamento leve/pesado e o fallback de `estimate_cost`
para dialetos sem `EXPLAIN` coberto. Sem rede: nada aqui abre conexão de verdade — o
fallback de dialeto é decidido só olhando `engine.dialect.name`, antes de qualquer I/O
(o caminho que de fato conversa com um Postgres real é
`tests/adapters/executors/test_cost_estimation.py`).
"""

from fixtures import vendas_detalhado

from adapters.executors.sqlalchemy_executor import SQLAlchemyQueryExecutor
from application.ports.query_executor import ExecutionProfile
from domain.models import Filter, FilterOperator, QueryRequest


class _FakeDialect:
    def __init__(self, name: str) -> None:
        self.name = name


class _FakeEngine:
    """Só o suficiente para `estimate_cost` decidir o fallback sem conectar."""

    def __init__(self, dialect_name: str) -> None:
        self.dialect = _FakeDialect(dialect_name)


def test_engine_e_timeout_seguem_o_profile():
    light = _FakeEngine("postgresql")
    heavy = _FakeEngine("postgresql")
    executor = SQLAlchemyQueryExecutor(
        light_engine=light,
        heavy_engine=heavy,
        light_timeout_seconds=5.0,
        heavy_timeout_seconds=300.0,
    )

    engine, timeout = executor._engine_and_timeout(ExecutionProfile.LIGHT)
    assert engine is light
    assert timeout == 5.0

    engine, timeout = executor._engine_and_timeout(ExecutionProfile.HEAVY)
    assert engine is heavy
    assert timeout == 300.0


async def test_dialeto_nao_postgres_cai_na_heuristica_sem_conectar():
    """Oracle (`vendas_detalhado`, seção 1.0) não tem caminho de `EXPLAIN` neste
    marco — `_FakeEngine` nunca precisa responder a `.connect()` porque a checagem de
    dialeto decide antes de qualquer tentativa de conexão."""
    oracle_engine = _FakeEngine("oracle")
    executor = SQLAlchemyQueryExecutor(light_engine=oracle_engine, heavy_engine=oracle_engine)
    dataset = vendas_detalhado()
    request = QueryRequest(
        schema="vendas", dimensions=("sigla_uf", "cargo"), measures=("valor_total",)
    )

    cost = await executor.estimate_cost(dataset, request)

    assert "heurística" in cost.detail
    assert cost.score == 2 * 10  # 2 dimensões, 0 filtros


async def test_heuristica_reduz_o_score_com_filtros():
    engine = _FakeEngine("oracle")
    executor = SQLAlchemyQueryExecutor(light_engine=engine, heavy_engine=engine)
    dataset = vendas_detalhado()
    request = QueryRequest(
        schema="vendas",
        dimensions=("sigla_uf",),
        measures=("valor_total",),
        filters=(Filter(field="sigla_uf", operator=FilterOperator.EQ, value="SP"),),
    )

    cost = await executor.estimate_cost(dataset, request)

    assert cost.score == max(1 * 10 - 1 * 5, 0)  # 1 dimensão, 1 filtro
