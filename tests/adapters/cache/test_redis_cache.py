"""`RedisCacheGateway` contra um Redis real (testcontainers) — o exemplo da seção 2.3,
TTL expirando de verdade, e a recusa de resultado não concluído que o port promete
desde o Marco 2.
"""

import asyncio
import logging
import shutil
import subprocess

import pytest
from redis.asyncio import Redis
from testcontainers.community.redis import RedisContainer

from adapters.cache.redis_cache import RedisCacheGateway
from application.ports.row_sink import StreamedResult
from domain.models import Column, DataType, QueryResult

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


def _resultado_da_secao_2_3(query_id: str) -> QueryResult:
    return QueryResult.completed(
        query_id=query_id,
        columns=(
            Column(field="sigla_uf", type=DataType.STRING),
            Column(field="valor_total", type=DataType.NUMBER, format="currency"),
            Column(field="quantidade", type=DataType.NUMBER),
        ),
        rows=(("SP", 458320.50, 1204), ("RJ", 212904.10, 588)),
        dataset_used="vendas_agregado_uf",
        execution_ms=12,
    )


@pytest.fixture(scope="module")
def redis_url():
    with RedisContainer("redis:7-alpine") as container:
        host = container.get_container_host_ip()
        port = int(container.get_exposed_port(6379))
        yield f"redis://{host}:{port}"


@pytest.fixture
async def redis_client(redis_url):
    client = Redis.from_url(redis_url, decode_responses=True)
    yield client
    await client.aclose()


async def _gravar(
    cache: RedisCacheGateway,
    key: str,
    result: QueryResult,
    *,
    ttl_seconds: int | None = None,
    chunk_size: int = 1,
) -> None:
    """Encena o worker: abre o sink, empurra as linhas em blocos e fecha.

    Substitui o antigo `cache.set(key, result)` — o port agora é alimentado bloco a
    bloco, e `chunk_size=1` exercita o caminho de várias escritas.
    """
    assert result.meta is not None
    sink = await cache.open_writer(
        key,
        result.columns,
        result.query_id,
        result.meta.dataset_used,
        ttl_seconds,
    )
    rows = list(result.rows or ())
    for start in range(0, len(rows), chunk_size):
        await sink.write(rows[start : start + chunk_size])
    await sink.close(
        StreamedResult(
            row_count=result.meta.row_count,
            total_rows=result.meta.total_rows,
            execution_ms=result.meta.execution_ms,
        )
    )



async def test_get_em_chave_ausente_devolve_none(redis_client):
    cache = RedisCacheGateway(redis_client)

    assert await cache.get("q_inexistente") is None


async def test_set_get_round_trip_do_exemplo_da_secao_2_3(redis_client):
    cache = RedisCacheGateway(redis_client)
    result = _resultado_da_secao_2_3("q_8f2a1c")

    await _gravar(cache, "q_8f2a1c", result)
    cached = await cache.get("q_8f2a1c")

    assert cached == result


async def test_delete_remove_a_entrada(redis_client):
    cache = RedisCacheGateway(redis_client)
    await _gravar(cache, "q_del", _resultado_da_secao_2_3("q_del"))

    await cache.delete("q_del")

    assert await cache.get("q_del") is None


async def test_abort_nao_deixa_entrada_no_redis(redis_client):
    """Onde antes o port recusava um `QueryResult` não concluído, agora não há resultado
    a recusar: o sink recebe linhas soltas. A garantia equivalente é o `abort` — uma
    consulta que falha no meio não pode deixar meia resposta no cache."""
    cache = RedisCacheGateway(redis_client, key_prefix="test_abort:")
    sink = await cache.open_writer(
        "q_abort", (Column(field="sigla_uf", type=DataType.STRING),), "q_abort", "ds"
    )
    await sink.write([("SP",)])

    await sink.abort()

    assert await cache.get("q_abort") is None


async def test_escrita_depois_do_abort_e_ignorada(redis_client):
    cache = RedisCacheGateway(redis_client, key_prefix="test_abort2:")
    sink = await cache.open_writer(
        "q_ab2", (Column(field="sigla_uf", type=DataType.STRING),), "q_ab2", "ds"
    )
    await sink.abort()

    await sink.write([("SP",)])  # não levanta
    assert await cache.get("q_ab2") is None


def _resultado_com_linhas(query_id: str, row_count: int) -> QueryResult:
    return QueryResult.completed(
        query_id=query_id,
        columns=(
            Column(field="sigla_uf", type=DataType.STRING),
            Column(field="valor_total", type=DataType.NUMBER),
        ),
        rows=tuple((f"UF{i}", float(i)) for i in range(row_count)),
        dataset_used="vendas_agregado_uf",
        execution_ms=1,
    )


async def test_resultado_acima_do_teto_de_linhas_nao_e_cacheado(redis_client, caplog):
    """Guardar um resultado gigante custa a memória do Redis e expulsa as consultas
    pequenas — que são as que se repetem. Não cachear não é erro: a próxima requisição
    igual simplesmente executa de novo."""
    cache = RedisCacheGateway(redis_client, key_prefix="test_max_rows:", max_rows=10)

    with caplog.at_level(logging.INFO):
        await _gravar(cache, "q_grande", _resultado_com_linhas("q_grande", 11))

    assert await redis_client.get("test_max_rows:q_grande") is None
    assert "não cacheado" in caplog.text
    # No teto ainda cabe — o corte é acima dele, não a partir dele.
    await _gravar(cache, "q_no_teto", _resultado_com_linhas("q_no_teto", 10))
    assert await cache.get("q_no_teto") is not None


async def test_payload_acima_do_teto_de_bytes_nao_e_cacheado(redis_client):
    """O segundo teto pega o que o de linhas não pega: poucas linhas muito largas — e é
    ele que corresponde ao limite de tamanho de valor do próprio Redis."""
    cache = RedisCacheGateway(
        redis_client, key_prefix="test_max_bytes:", max_rows=None, max_payload_bytes=200
    )

    await _gravar(cache, "q_largo", _resultado_com_linhas("q_largo", 50))

    assert await redis_client.get("test_max_bytes:q_largo") is None


async def test_tetos_desligados_gravam_qualquer_resultado(redis_client):
    cache = RedisCacheGateway(
        redis_client, key_prefix="test_sem_teto:", max_rows=None, max_payload_bytes=None
    )
    result = _resultado_com_linhas("q_sem_teto", 500)

    await _gravar(cache, "q_sem_teto", result)

    assert await cache.get("q_sem_teto") == result


async def test_ttl_expira_de_verdade(redis_client):
    cache = RedisCacheGateway(redis_client)
    result = _resultado_da_secao_2_3("q_ttl")

    await _gravar(cache, "q_ttl", result, ttl_seconds=1)
    assert await cache.get("q_ttl") == result

    await asyncio.sleep(1.5)

    assert await cache.get("q_ttl") is None


async def test_ttl_padrao_e_aplicado_quando_nao_especificado(redis_client):
    cache = RedisCacheGateway(redis_client, default_ttl_seconds=60)
    await _gravar(cache, "q_default_ttl", _resultado_da_secao_2_3("q_default_ttl"))

    ttl = await redis_client.ttl(cache._redis_key("q_default_ttl"))

    assert 0 < ttl <= 60


async def test_chave_usa_o_prefixo_configurado(redis_client):
    cache = RedisCacheGateway(redis_client, key_prefix="test:")
    await _gravar(cache, "q_1", _resultado_da_secao_2_3("q_1"))

    raw = await redis_client.get("test:q_1")

    assert raw is not None


async def test_chave_carrega_o_schema_de_origem(redis_client):
    """A chave é `QueryRequest.cache_key` (`<schema>:<query_id>`) sob o prefixo."""
    cache = RedisCacheGateway(redis_client, key_prefix="test_ns:")
    await _gravar(cache, "vendas:q_ns1", _resultado_da_secao_2_3("q_ns1"))

    assert await redis_client.get("test_ns:vendas:q_ns1") is not None


async def test_clear_por_schema_so_apaga_o_prefixo(redis_client):
    cache = RedisCacheGateway(redis_client, key_prefix="test_clear_schema:")
    await _gravar(cache, "vendas:q_a", _resultado_da_secao_2_3("q_a"))
    await _gravar(cache, "vendas:q_b", _resultado_da_secao_2_3("q_b"))
    await _gravar(cache, "rh:q_c", _resultado_da_secao_2_3("q_c"))

    removed = await cache.clear("vendas")

    assert removed == 2
    assert await cache.get("vendas:q_a") is None
    assert await cache.get("rh:q_c") is not None


async def test_clear_sem_schema_apaga_todo_o_cache_mas_nao_os_contadores(redis_client):
    cache = RedisCacheGateway(redis_client, key_prefix="test_clear_all:")
    await _gravar(cache, "vendas:q_a", _resultado_da_secao_2_3("q_a"))
    await _gravar(cache, "rh:q_b", _resultado_da_secao_2_3("q_b"))
    await cache.get("vendas:q_a")  # incrementa cache:hits
    hits_antes = (await cache.stats()).hits

    removed = await cache.clear()

    assert removed == 2
    assert await cache.get("vendas:q_a") is None
    # `cache:hits`/`cache:misses` não têm o prefixo `query:`/`test_clear_all:` — o
    # `clear` não os toca (a chamada `get` acima soma 1 miss ao contador).
    assert (await cache.stats()).hits == hits_antes


async def test_stats_acompanha_hits_e_misses_reais(redis_client):
    """`cache:hits`/`cache:misses` são chaves fixas, compartilhadas por todo o
    processo (Marco 9) — os demais testes deste módulo também as incrementam, então a
    asserção é sobre a variação, não sobre um valor absoluto."""
    cache = RedisCacheGateway(redis_client, key_prefix="test_stats:")
    before = await cache.stats()

    await cache.get("q_stats_ausente")  # miss
    await _gravar(cache, "q_stats_presente", _resultado_da_secao_2_3("q_stats_presente"))
    await cache.get("q_stats_presente")  # hit

    after = await cache.stats()

    assert after.misses == before.misses + 1
    assert after.hits == before.hits + 1
