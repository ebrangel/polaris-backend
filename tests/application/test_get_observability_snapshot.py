"""`GetObservabilitySnapshot` — junta `CacheGateway.stats()` e `JobQueue.depth()` num
único snapshot (Marco 9), sem nenhum port novo só para leitura de métricas.
"""

from application.use_cases.get_observability_snapshot import GetObservabilitySnapshot
from domain.models import Column, DataType, QueryRequest, QueryResult
from fakes import InMemoryCacheGateway, InMemoryJobQueue


async def test_snapshot_sem_nenhum_trafego():
    snapshot = await GetObservabilitySnapshot(
        cache=InMemoryCacheGateway(), job_queue=InMemoryJobQueue()
    )()

    assert snapshot.cache_hits == 0
    assert snapshot.cache_misses == 0
    assert snapshot.cache_hit_rate == 0.0  # não é divisão por zero
    assert snapshot.heavy_queue_depth == 0


async def test_snapshot_calcula_a_taxa_de_acerto():
    cache = InMemoryCacheGateway()
    await cache.get("q_ausente")  # miss
    await cache.get("q_ausente")  # miss
    await cache.get("q_ausente")  # miss
    # 1 hit: grava e lê de volta.
    result = QueryResult.completed(
        query_id="q_presente",
        columns=(Column(field="sigla_uf", type=DataType.STRING),),
        rows=(("SP",),),
        dataset_used="vendas_agregado_uf",
    )
    await cache.set("q_presente", result)
    await cache.get("q_presente")  # hit

    snapshot = await GetObservabilitySnapshot(cache=cache, job_queue=InMemoryJobQueue())()

    assert snapshot.cache_hits == 1
    assert snapshot.cache_misses == 3
    assert snapshot.cache_hit_rate == 0.25


async def test_snapshot_reflete_a_profundidade_da_fila():
    job_queue = InMemoryJobQueue()
    await job_queue.enqueue(
        QueryRequest(schema="vendas", dimensions=("sigla_uf",)), "vendas_agregado_uf"
    )
    await job_queue.enqueue(
        QueryRequest(schema="vendas", measures=("valor_total",)), "vendas_agregado_uf"
    )

    snapshot = await GetObservabilitySnapshot(cache=InMemoryCacheGateway(), job_queue=job_queue)()

    assert snapshot.heavy_queue_depth == 2
