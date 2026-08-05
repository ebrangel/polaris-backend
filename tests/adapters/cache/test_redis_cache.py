"""`RedisCacheGateway` contra um Redis real (testcontainers) — o exemplo da seção 2.3,
TTL expirando de verdade, e a recusa de resultado não concluído que o port promete
desde o Marco 2.
"""

import asyncio
import shutil
import subprocess

import pytest
from redis.asyncio import Redis
from testcontainers.community.redis import RedisContainer

from adapters.cache.redis_cache import RedisCacheGateway
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


async def test_get_em_chave_ausente_devolve_none(redis_client):
    cache = RedisCacheGateway(redis_client)

    assert await cache.get("q_inexistente") is None


async def test_set_get_round_trip_do_exemplo_da_secao_2_3(redis_client):
    cache = RedisCacheGateway(redis_client)
    result = _resultado_da_secao_2_3("q_8f2a1c")

    await cache.set("q_8f2a1c", result)
    cached = await cache.get("q_8f2a1c")

    assert cached == result


async def test_delete_remove_a_entrada(redis_client):
    cache = RedisCacheGateway(redis_client)
    await cache.set("q_del", _resultado_da_secao_2_3("q_del"))

    await cache.delete("q_del")

    assert await cache.get("q_del") is None


async def test_recusa_resultado_nao_concluido(redis_client):
    cache = RedisCacheGateway(redis_client)

    with pytest.raises(ValueError, match="completed"):
        await cache.set("q_proc", QueryResult.processing("q_proc"))

    assert await cache.get("q_proc") is None


async def test_ttl_expira_de_verdade(redis_client):
    cache = RedisCacheGateway(redis_client)
    result = _resultado_da_secao_2_3("q_ttl")

    await cache.set("q_ttl", result, ttl_seconds=1)
    assert await cache.get("q_ttl") == result

    await asyncio.sleep(1.5)

    assert await cache.get("q_ttl") is None


async def test_ttl_padrao_e_aplicado_quando_nao_especificado(redis_client):
    cache = RedisCacheGateway(redis_client, default_ttl_seconds=60)
    await cache.set("q_default_ttl", _resultado_da_secao_2_3("q_default_ttl"))

    ttl = await redis_client.ttl(cache._redis_key("q_default_ttl"))

    assert 0 < ttl <= 60


async def test_chave_usa_o_prefixo_configurado(redis_client):
    cache = RedisCacheGateway(redis_client, key_prefix="test:")
    await cache.set("q_1", _resultado_da_secao_2_3("q_1"))

    raw = await redis_client.get("test:q_1")

    assert raw is not None
