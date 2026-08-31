"""`run_heavy_query` e `build_worker_settings` — chamados diretamente, sem worker nem
Redis: a task é um shim fino sobre `RunQueuedQuery`, testável isoladamente.
"""

import pytest

from adapters.queue.tasks import build_worker_settings, purge_exports, run_heavy_query
from adapters.serialization import request_to_dict
from domain.models import Column, DataType, QueryRequest, QueryResult
from fakes import InMemoryResultExporter


class _FakeRunQueuedQuery:
    """Substitui `RunQueuedQuery` — não precisa de `Catalog` nem de executores reais
    para provar que o shim desserializa/serializa direito."""

    def __init__(self, result: QueryResult) -> None:
        self.result = result
        self.calls: list[tuple[QueryRequest, str]] = []

    async def __call__(self, request: QueryRequest, dataset_name: str) -> QueryResult:
        self.calls.append((request, dataset_name))
        return self.result


async def test_run_heavy_query_delega_para_o_use_case_no_ctx():
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))
    result = QueryResult.completed(
        query_id=request.query_id,
        columns=(Column(field="sigla_uf", type=DataType.STRING),),
        rows=(("SP",),),
        dataset_used="vendas_agregado_uf",
    )
    fake = _FakeRunQueuedQuery(result)
    ctx = {"run_queued_query": fake}

    body = await run_heavy_query(ctx, request_to_dict(request), "vendas_agregado_uf")

    assert fake.calls == [(request, "vendas_agregado_uf")]
    assert body["query_id"] == request.query_id
    assert body["status"] == "completed"
    assert body["rows"] == [["SP"]]


async def test_build_worker_settings_registra_a_task_e_o_queue_name():
    fake = _FakeRunQueuedQuery(QueryResult.processing("q_1"))

    settings = build_worker_settings(fake, redis_settings="fake-redis-settings", queue_name="minha_fila")

    assert settings.functions == [run_heavy_query]
    assert settings.queue_name == "minha_fila"
    assert settings.redis_settings == "fake-redis-settings"


async def test_on_startup_disponibiliza_o_use_case_no_ctx():
    fake = _FakeRunQueuedQuery(QueryResult.processing("q_1"))
    settings = build_worker_settings(fake, redis_settings="fake-redis-settings")

    ctx: dict = {}
    await settings.on_startup(ctx)

    assert ctx["run_queued_query"] is fake


async def test_ciclo_completo_enqueue_shape_ate_o_shim():
    """`run_heavy_query(ctx, *args)` é chamado pelo worker exatamente com os dois
    argumentos posicionais que `ArqJobQueue.enqueue` passa para `enqueue_job`."""
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",))
    result = QueryResult.completed(
        query_id=request.query_id,
        columns=(Column(field="sigla_uf", type=DataType.STRING),),
        rows=(),
        dataset_used="vendas_agregado_uf",
    )
    fake = _FakeRunQueuedQuery(result)
    settings = build_worker_settings(fake, redis_settings="fake-redis-settings")
    ctx: dict = {}
    await settings.on_startup(ctx)

    body = await settings.functions[0](ctx, request_to_dict(request), "vendas_agregado_uf")

    assert body["status"] == "completed"


# --- Varredura de exports (seção 2.4a) --------------------------------------------------


async def test_purge_exports_delega_para_o_exportador():
    exporter = InMemoryResultExporter(ttl_seconds=3600)
    result = QueryResult.completed(
        query_id="q_8f2a1c",
        columns=(Column(field="sigla_uf", type=DataType.STRING),),
        rows=(("SP",),),
        dataset_used="vendas_agregado_uf",
    )
    await exporter.export(result)
    exporter.expire("q_8f2a1c")

    removidos = await purge_exports({"result_exporter": exporter})

    assert removidos == 1
    assert await exporter.stat("q_8f2a1c") is None


async def test_cron_de_limpeza_registrado_quando_ha_exportador():
    fake = _FakeRunQueuedQuery(QueryResult.processing("q_1"))
    exporter = InMemoryResultExporter()

    settings = build_worker_settings(
        fake, redis_settings="fake-redis-settings", result_exporter=exporter
    )

    assert [job.coroutine for job in settings.cron_jobs] == [purge_exports]
    ctx: dict = {}
    await settings.on_startup(ctx)
    assert ctx["result_exporter"] is exporter


async def test_sem_exportador_nao_registra_cron():
    """Um cron que só levantaria `KeyError` no `ctx` é pior que cron nenhum."""
    fake = _FakeRunQueuedQuery(QueryResult.processing("q_1"))

    settings = build_worker_settings(fake, redis_settings="fake-redis-settings")

    assert settings.cron_jobs == []


async def test_exportador_e_provider_juntos_sao_recusados():
    fake = _FakeRunQueuedQuery(QueryResult.processing("q_1"))

    async def provider():
        return InMemoryResultExporter()

    with pytest.raises(ValueError, match="no máximo um"):
        build_worker_settings(
            fake,
            redis_settings="fake-redis-settings",
            result_exporter=InMemoryResultExporter(),
            result_exporter_provider=provider,
        )


async def test_provider_de_exportador_e_aguardado_no_startup():
    fake = _FakeRunQueuedQuery(QueryResult.processing("q_1"))
    exporter = InMemoryResultExporter()

    async def provider():
        return exporter

    settings = build_worker_settings(
        fake, redis_settings="fake-redis-settings", result_exporter_provider=provider
    )
    ctx: dict = {}
    await settings.on_startup(ctx)

    assert ctx["result_exporter"] is exporter
