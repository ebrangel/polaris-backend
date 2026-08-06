"""`RunQueuedQuery` — o lado worker da seção 2.4: recebe o dataset já resolvido pelo
nome (não chama `ResolveDataset` de novo) e executa no perfil `HEAVY`.
"""

import pytest
from fixtures import catalog, vendas_schema

from application.ports.query_executor import ExecutionProfile
from application.use_cases.run_queued_query import RunQueuedQuery
from domain.models import Catalog, QueryRequest, QueryStatus
from fakes import StubQueryExecutor


async def test_executa_no_perfil_heavy():
    stub = StubQueryExecutor()
    run = RunQueuedQuery(catalog=catalog(), executors={"env:DW_VENDAS_PG_URL": stub})
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))

    result = await run(request, dataset_name="vendas_agregado_uf")

    assert result.status is QueryStatus.COMPLETED
    dataset, domain_request, columns, profile = stub.calls[0]
    assert dataset.name == "vendas_agregado_uf"
    assert domain_request == request
    assert profile is ExecutionProfile.HEAVY


async def test_resolve_o_dataset_pelo_nome_nao_pela_cobertura():
    """`vendas_detalhado` (Oracle) é o segundo dataset do schema `vendas` — pedir só
    `sigla_uf` normalmente resolveria para `vendas_agregado_uf` via `ResolveDataset`,
    mas aqui o nome já veio decidido no payload do job."""
    postgres_stub = StubQueryExecutor()
    oracle_stub = StubQueryExecutor()
    run = RunQueuedQuery(
        catalog=Catalog(schemas={"vendas": vendas_schema()}),
        executors={"env:DW_VENDAS_PG_URL": postgres_stub, "env:DW_VENDAS_ORACLE_URL": oracle_stub},
    )
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))

    await run(request, dataset_name="vendas_detalhado")

    assert len(oracle_stub.calls) == 1
    assert len(postgres_stub.calls) == 0


async def test_dataset_inexistente_no_schema():
    run = RunQueuedQuery(catalog=catalog(), executors={})
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",))

    with pytest.raises(LookupError, match="inexistente"):
        await run(request, dataset_name="inexistente")


async def test_executor_nao_configurado():
    run = RunQueuedQuery(catalog=catalog(), executors={})
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",))

    with pytest.raises(LookupError, match="vendas_agregado_uf"):
        await run(request, dataset_name="vendas_agregado_uf")


async def test_erro_do_executor_propaga():
    from domain.errors import QueryTimeoutError

    stub = StubQueryExecutor(raises=QueryTimeoutError("estourou"))
    run = RunQueuedQuery(catalog=catalog(), executors={"env:DW_VENDAS_PG_URL": stub})
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",))

    with pytest.raises(QueryTimeoutError):
        await run(request, dataset_name="vendas_agregado_uf")
