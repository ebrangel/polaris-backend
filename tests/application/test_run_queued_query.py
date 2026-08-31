"""`RunQueuedQuery` — o lado worker da seção 2.4: recebe o dataset já resolvido pelo
nome (não chama `ResolveDataset` de novo) e executa no perfil `HEAVY`.
"""

import logging

import pytest
from fixtures import catalog, vendas_schema

from application.ports.query_executor import ExecutionProfile
from application.use_cases.run_queued_query import RunQueuedQuery
from domain.models import Catalog, QueryRequest, QueryResult, QueryStatus
from fakes import InMemoryResultExporter, StubQueryExecutor


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


# --- Log de consultas lentas (Marco 9) ------------------------------------------------------


async def test_consulta_pesada_lenta_gera_log_de_warning(caplog):
    slow_result = QueryResult.completed(
        query_id="q_pesado1",
        columns=(),
        rows=(),
        dataset_used="vendas_agregado_uf",
        execution_ms=9000,
    )
    stub = StubQueryExecutor(result=slow_result)
    run = RunQueuedQuery(
        catalog=catalog(),
        executors={"env:DW_VENDAS_PG_URL": stub},
        slow_query_threshold_ms=5000,
    )
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",))

    with caplog.at_level(logging.WARNING):
        await run(request, dataset_name="vendas_agregado_uf")

    assert any("consulta lenta" in record.message for record in caplog.records)
    assert any("q_pesado1" in record.message for record in caplog.records)


async def test_sem_threshold_configurado_nunca_loga(caplog):
    slow_result = QueryResult.completed(
        query_id="q_pesado2",
        columns=(),
        rows=(),
        dataset_used="vendas_agregado_uf",
        execution_ms=999_999,
    )
    stub = StubQueryExecutor(result=slow_result)
    run = RunQueuedQuery(catalog=catalog(), executors={"env:DW_VENDAS_PG_URL": stub})
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",))

    with caplog.at_level(logging.WARNING):
        await run(request, dataset_name="vendas_agregado_uf")

    assert caplog.records == []


# --- Export do resultado (seção 2.4a) ---------------------------------------------------


async def test_exporta_o_resultado_apos_executar():
    """Todo job pesado concluído deixa um arquivo baixável — não só os que pediram CSV.
    O formato de saída não faz parte da `QueryRequest` (seção 2.3a), e o `arq` deduplica
    jobs por `query_id`: condicionar o export à intenção do cliente perderia a intenção
    da segunda de duas requisições idênticas."""
    exporter = InMemoryResultExporter()
    run = RunQueuedQuery(
        catalog=catalog(),
        executors={"env:DW_VENDAS_PG_URL": StubQueryExecutor()},
        result_exporter=exporter,
    )
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))

    result = await run(request, dataset_name="vendas_agregado_uf")

    assert exporter.calls == [result.query_id]
    assert await exporter.stat(result.query_id) is not None


async def test_sem_exportador_configurado_nao_exporta():
    """Comportamento anterior ao export, preservado: a consulta roda e volta pela fila."""
    run = RunQueuedQuery(
        catalog=catalog(), executors={"env:DW_VENDAS_PG_URL": StubQueryExecutor()}
    )
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))

    result = await run(request, dataset_name="vendas_agregado_uf")

    assert result.status is QueryStatus.COMPLETED


async def test_falha_de_export_nao_derruba_o_job(caplog):
    """O resultado já foi calculado — o cliente perde o link de download, não a
    resposta."""
    exporter = InMemoryResultExporter(raises=OSError("disco cheio"))
    run = RunQueuedQuery(
        catalog=catalog(),
        executors={"env:DW_VENDAS_PG_URL": StubQueryExecutor()},
        result_exporter=exporter,
    )
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))

    with caplog.at_level(logging.WARNING):
        result = await run(request, dataset_name="vendas_agregado_uf")

    assert result.status is QueryStatus.COMPLETED
    assert "falha ao exportar" in caplog.text
