"""`ArqJobQueue` contra um Redis real (testcontainers) — enqueue/status/depth, a
deduplicação por `query_id` e o ciclo completo até `completed` via um worker `arq` de
verdade rodando em modo `burst` (processa o que está na fila e para; sem processo
separado, sem polling).
"""

import shutil
import subprocess
from typing import Any

import pytest
from arq.connections import ArqRedis, RedisSettings, create_pool
from arq.worker import Worker, func
from testcontainers.community.redis import RedisContainer

from adapters.queue.arq_queue import ArqJobQueue
from adapters.serialization import result_to_dict
from domain.models import Column, DataType, QueryRequest, QueryResult, QueryStatus

pytestmark = pytest.mark.integration


def _docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        subprocess.run(["docker", "info"], check=True, capture_output=True, timeout=5)
        return True
    except Exception:
        return False


if not _docker_available():
    pytest.skip("Docker indisponível — pulando testes de integração", allow_module_level=True)


@pytest.fixture(scope="module")
def redis_settings():
    with RedisContainer("redis:7-alpine") as container:
        host = container.get_container_host_ip()
        port = int(container.get_exposed_port(6379))
        yield RedisSettings(host=host, port=port)


@pytest.fixture
async def pool(redis_settings) -> ArqRedis:
    p = await create_pool(redis_settings)
    await p.flushdb()  # isola cada teste, mesmo container Redis reaproveitado no módulo
    yield p
    await p.aclose()


def _request(**kwargs) -> QueryRequest:
    return QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",), **kwargs)


async def test_enqueue_devolve_o_corpo_do_202_da_secao_2_4(pool):
    queue = ArqJobQueue(pool)
    request = _request()

    result = await queue.enqueue(request, dataset_name="vendas_agregado_uf")

    assert result.status is QueryStatus.PROCESSING
    assert result.query_id == request.query_id


async def test_depth_reflete_os_jobs_pendentes(pool):
    queue = ArqJobQueue(pool)
    assert await queue.depth() == 0

    await queue.enqueue(_request(limit=1), dataset_name="vendas_agregado_uf")
    await queue.enqueue(_request(limit=2), dataset_name="vendas_agregado_uf")

    assert await queue.depth() == 2


async def test_get_status_logo_apos_enqueue_e_processing(pool):
    queue = ArqJobQueue(pool)
    request = _request()
    await queue.enqueue(request, dataset_name="vendas_agregado_uf")

    status = await queue.get_status(request.query_id)

    assert status is not None
    assert status.status is QueryStatus.PROCESSING


async def test_query_id_desconhecido_devolve_none(pool):
    queue = ArqJobQueue(pool)

    assert await queue.get_status("q_nunca_existiu") is None


async def test_wait_for_result_timeout_devolve_processing(pool):
    """Sem worker consumindo a fila, a janela expira e a espera inline devolve
    `processing` — a borda HTTP responde 202 + poll_url."""
    queue = ArqJobQueue(pool)
    request = _request()
    await queue.enqueue(request, dataset_name="vendas_agregado_uf")

    result = await queue.wait_for_result(request.query_id, timeout=0.2)

    assert result.status is QueryStatus.PROCESSING
    assert result.query_id == request.query_id


async def test_wait_for_result_query_id_desconhecido_devolve_processing(pool):
    queue = ArqJobQueue(pool)

    result = await queue.wait_for_result("q_nunca_existiu", timeout=0.2)

    assert result.status is QueryStatus.PROCESSING


async def test_wait_for_result_completed_apos_worker_burst(pool):
    request = _request()
    expected_result = QueryResult.completed(
        query_id=request.query_id,
        columns=(Column(field="sigla_uf", type=DataType.STRING),),
        rows=(("SP",),),
        dataset_used="vendas_agregado_uf",
    )

    async def run_heavy_query(ctx: dict[str, Any], request_dict: dict, dataset_name: str) -> dict:
        return result_to_dict(expected_result)

    queue = ArqJobQueue(pool)
    await queue.enqueue(request, dataset_name="vendas_agregado_uf")

    worker = Worker(
        functions=[func(run_heavy_query, name="run_heavy_query")],
        redis_pool=pool,
        burst=True,
        poll_delay=0,
    )
    await worker.async_run()
    await worker.close()

    result = await queue.wait_for_result(request.query_id, timeout=5)

    assert result == expected_result


async def test_wait_for_result_failed_apos_worker_burst(pool):
    request = _request()

    async def run_heavy_query(ctx: dict[str, Any], request_dict: dict, dataset_name: str) -> dict:
        raise RuntimeError("estourou o timeout do datasource")

    queue = ArqJobQueue(pool)
    await queue.enqueue(request, dataset_name="vendas_agregado_uf")

    worker = Worker(
        functions=[func(run_heavy_query, name="run_heavy_query")],
        redis_pool=pool,
        burst=True,
        poll_delay=0,
    )
    await worker.async_run()
    await worker.close()

    result = await queue.wait_for_result(request.query_id, timeout=5)

    assert result.status is QueryStatus.FAILED
    assert "estourou o timeout" in result.error


async def test_enfileirar_o_mesmo_query_id_duas_vezes_nao_duplica_o_job(pool):
    """`query_id` como `_job_id`: duas requisições idênticas (mesmo hash — seção 3)
    reaproveitam o job já na fila."""
    queue = ArqJobQueue(pool)
    request = _request()

    await queue.enqueue(request, dataset_name="vendas_agregado_uf")
    await queue.enqueue(request, dataset_name="vendas_agregado_uf")

    assert await queue.depth() == 1


async def test_ciclo_completo_ate_completed_via_worker_burst(pool):
    """Sem processo separado: `Worker(..., burst=True)` processa o que está na fila,
    no mesmo event loop do teste, e sai — é o padrão de teste do próprio `arq`."""
    request = _request()
    expected_result = QueryResult.completed(
        query_id=request.query_id,
        columns=(Column(field="sigla_uf", type=DataType.STRING),),
        rows=(("SP",),),
        dataset_used="vendas_agregado_uf",
    )

    async def run_heavy_query(ctx: dict[str, Any], request_dict: dict, dataset_name: str) -> dict:
        assert dataset_name == "vendas_agregado_uf"
        return result_to_dict(expected_result)

    queue = ArqJobQueue(pool)
    await queue.enqueue(request, dataset_name="vendas_agregado_uf")

    # `func(..., name=...)`: por padrão o arq registra a task pelo `__qualname__` da
    # coroutine — para uma closure definida dentro do teste isso seria
    # "test_x.<locals>.run_heavy_query", não batendo com o nome literal
    # ("run_heavy_query") que `ArqJobQueue.enqueue` gravou no Redis.
    worker = Worker(
        functions=[func(run_heavy_query, name="run_heavy_query")],
        redis_pool=pool,
        burst=True,
        poll_delay=0,
    )
    await worker.async_run()
    await worker.close()

    status = await queue.get_status(request.query_id)

    assert status == expected_result


async def test_ciclo_completo_ate_failed_via_worker_burst(pool):
    request = _request()

    async def run_heavy_query(ctx: dict[str, Any], request_dict: dict, dataset_name: str) -> dict:
        raise RuntimeError("estourou o timeout do datasource")

    queue = ArqJobQueue(pool)
    await queue.enqueue(request, dataset_name="vendas_agregado_uf")

    # `func(..., name=...)`: por padrão o arq registra a task pelo `__qualname__` da
    # coroutine — para uma closure definida dentro do teste isso seria
    # "test_x.<locals>.run_heavy_query", não batendo com o nome literal
    # ("run_heavy_query") que `ArqJobQueue.enqueue` gravou no Redis.
    worker = Worker(
        functions=[func(run_heavy_query, name="run_heavy_query")],
        redis_pool=pool,
        burst=True,
        poll_delay=0,
    )
    await worker.async_run()
    await worker.close()

    status = await queue.get_status(request.query_id)

    assert status is not None
    assert status.status is QueryStatus.FAILED
    assert "estourou o timeout" in status.error


async def test_enqueue_apos_job_concluido_descarta_o_resultado_retido_e_re_executa(pool):
    """Depois que um job idêntico já concluiu (resultado retido pelo arq, `keep_result`),
    um novo `enqueue` do mesmo `query_id` precisa re-executar — senão `wait_for_result`
    devolveria o resultado antigo, mascarando um `CacheGateway` que foi purgado nesse
    meio tempo (a consulta voltaria como `cached=false` e o cache nunca se repovoaria)."""
    request = _request()
    calls: list[int] = []

    async def run_heavy_query(ctx: dict[str, Any], request_dict: dict, dataset_name: str) -> dict:
        calls.append(1)
        return result_to_dict(
            QueryResult.completed(
                query_id=request.query_id,
                columns=(Column(field="sigla_uf", type=DataType.STRING),),
                rows=(("SP",),),
                dataset_used="vendas_agregado_uf",
            )
        )

    def _worker() -> Worker:
        return Worker(
            functions=[func(run_heavy_query, name="run_heavy_query")],
            redis_pool=pool,
            burst=True,
            poll_delay=0,
        )

    queue = ArqJobQueue(pool)

    await queue.enqueue(request, dataset_name="vendas_agregado_uf")
    w1 = _worker()
    await w1.async_run()
    await w1.close()
    assert calls == [1]
    assert await queue.get_status(request.query_id) is not None  # resultado retido

    # Mesmo `query_id`, cache purgado: tem de virar um job novo, não devolver o antigo.
    await queue.enqueue(request, dataset_name="vendas_agregado_uf")
    assert await queue.depth() == 1

    w2 = _worker()
    await w2.async_run()
    await w2.close()
    assert calls == [1, 1]  # re-executou de fato
