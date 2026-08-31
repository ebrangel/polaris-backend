"""`SQLAlchemyQueryExecutor` — roteamento leve/pesado, o `EXPLAIN PLAN` do Oracle e o
fallback de `estimate_cost` para dialetos sem `EXPLAIN` coberto. Sem rede: o Oracle é
representado por uma conexão falsa que devolve linhas da `PLAN_TABLE` (não há container
de Oracle neste projeto, mesma limitação do Marco 5); o caminho que de fato conversa com
um banco real é `tests/adapters/executors/test_cost_estimation.py`, contra Postgres.
"""

import pytest
from fixtures import vendas_agregado_uf, vendas_detalhado
from sqlalchemy.dialects import oracle

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


class _FakeResult:
    def __init__(self, row: tuple | None) -> None:
        self._row = row

    def first(self) -> tuple | None:
        return self._row


class _FakeConnection:
    """Conexão que registra o SQL executado e devolve a linha programada da `PLAN_TABLE`."""

    def __init__(self, plan_row: tuple | None, raises: Exception | None = None) -> None:
        self.plan_row = plan_row
        self.raises = raises
        self.statements: list[str] = []

    async def __aenter__(self) -> "_FakeConnection":
        return self

    async def __aexit__(self, *exc_info) -> None:
        return None

    async def exec_driver_sql(self, sql: str) -> _FakeResult:
        self.statements.append(sql)
        if self.raises is not None:
            raise self.raises
        return _FakeResult(self.plan_row if sql.startswith("SELECT COST") else None)


class _FakeOracleEngine:
    """`engine.connect()` como o SQLAlchemy o expõe: um context manager assíncrono.

    O dialeto é o de verdade (`sqlalchemy.dialects.oracle`), e não um dublê, porque é
    ele que compila o `Select` com `literal_binds` — o único jeito de o teste garantir
    que o SQL embutido no `EXPLAIN PLAN` é o SQL Oracle de verdade.
    """

    def __init__(self, plan_row: tuple | None = (4200.0, 1_800_000), raises=None) -> None:
        self.dialect = oracle.dialect()
        self.connection = _FakeConnection(plan_row, raises)

    def connect(self) -> _FakeConnection:
        return self.connection


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


async def test_dialeto_sem_explain_coberto_cai_na_heuristica_sem_conectar():
    """Um dialeto fora de Postgres/Oracle não tem caminho de `EXPLAIN` — `_FakeEngine`
    nunca precisa responder a `.connect()` porque a checagem de dialeto decide antes de
    qualquer tentativa de conexão."""
    engine = _FakeEngine("sqlite")
    executor = SQLAlchemyQueryExecutor(light_engine=engine, heavy_engine=engine)
    dataset = vendas_detalhado()
    request = QueryRequest(
        schema="vendas", dimensions=("sigla_uf", "cargo"), measures=("valor_total",)
    )

    cost = await executor.estimate_cost(dataset, request)

    assert "heurística" in cost.detail
    assert cost.score == 2 * 10  # 2 dimensões, 0 filtros


async def test_heuristica_reduz_o_score_com_filtros():
    engine = _FakeEngine("sqlite")
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


async def test_heuristica_e_julgada_por_limiar_da_propria_escala():
    """O score da heurística é contagem de campos (dezenas); o `cost_threshold` é custo
    de otimizador (milhares). Compará-los tornava *toda* consulta sem `EXPLAIN` leve, e
    portanto síncrona, dentro do processo da API."""
    engine = _FakeEngine("sqlite")
    executor = SQLAlchemyQueryExecutor(
        light_engine=engine,
        heavy_engine=engine,
        cost_threshold=10_000.0,
        heuristic_threshold=30.0,
    )
    dataset = vendas_detalhado()
    muitas_dimensoes = QueryRequest(
        schema="vendas", dimensions=("sigla_uf", "cargo", "canal", "produto")
    )

    cost = await executor.estimate_cost(dataset, muitas_dimensoes)

    assert cost.threshold == 30.0  # e não os 10 000 do EXPLAIN
    assert cost.score == 4 * 10
    assert cost.is_heavy is True


# --- EXPLAIN PLAN do Oracle -------------------------------------------------------------


async def test_oracle_estima_custo_pela_plan_table():
    engine = _FakeOracleEngine(plan_row=(4200.0, 1_800_000))
    executor = SQLAlchemyQueryExecutor(
        light_engine=engine, heavy_engine=engine, cost_threshold=1_000.0
    )
    request = QueryRequest(
        schema="vendas", dimensions=("sigla_uf", "cargo"), measures=("valor_total",)
    )

    cost = await executor.estimate_cost(vendas_detalhado(), request)

    assert cost.score == 4200.0
    assert cost.threshold == 1_000.0
    assert cost.is_heavy is True  # com a heurística antiga, seria leve
    assert "Cardinality = 1800000" in cost.detail

    explain, select, delete = engine.connection.statements
    assert explain.startswith("EXPLAIN PLAN SET STATEMENT_ID = 'polaris_")
    # O SQL de verdade, compilado pelo dialeto Oracle (daí os identificadores citados).
    assert '"SCHEMA_DW"."FT_VENDAS"' in explain
    assert select.startswith("SELECT COST, CARDINALITY FROM PLAN_TABLE")
    # Mesmo `STATEMENT_ID` nos três comandos, e as linhas saem da tabela de sessão.
    statement_id = explain.split("'")[1]
    assert statement_id in select
    assert delete == f"DELETE FROM PLAN_TABLE WHERE STATEMENT_ID = '{statement_id}'"


@pytest.mark.parametrize(
    "engine_factory",
    [
        lambda: _FakeOracleEngine(plan_row=None),  # nenhuma linha na PLAN_TABLE
        lambda: _FakeOracleEngine(plan_row=(None, 10)),  # sem estatísticas: COST nulo
        lambda: _FakeOracleEngine(raises=RuntimeError("ORA-02404: PLAN_TABLE ausente")),
    ],
    ids=["sem_linha", "custo_nulo", "erro_no_explain"],
)
async def test_oracle_sem_plano_utilizavel_cai_na_heuristica(engine_factory):
    """Estimar custo nunca derruba consulta que funcionaria — falta de `PLAN_TABLE`,
    falta de estatísticas ou erro de permissão viram heurística, não erro."""
    executor = SQLAlchemyQueryExecutor(
        light_engine=engine_factory(), heavy_engine=_FakeEngine("oracle")
    )
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",))

    cost = await executor.estimate_cost(vendas_detalhado(), request)

    assert "heurística" in cost.detail
    assert cost.score == 1 * 10


async def test_dataset_postgres_nao_usa_o_caminho_do_oracle():
    """O dialeto que decide é o do engine, não o `datasource.type` do dataset."""
    engine = _FakeEngine("postgresql")
    executor = SQLAlchemyQueryExecutor(light_engine=engine, heavy_engine=engine)

    cost = await executor.estimate_cost(
        vendas_agregado_uf(), QueryRequest(schema="vendas", dimensions=("sigla_uf",))
    )

    # `_FakeEngine` não tem `.connect()`: o EXPLAIN do Postgres falha e cai na heurística
    # — o que importa aqui é que nenhum `EXPLAIN PLAN` de Oracle foi tentado.
    assert "heurística" in cost.detail
