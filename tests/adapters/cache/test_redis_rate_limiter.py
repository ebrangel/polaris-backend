"""`RedisRateLimiter` contra um Redis real (testcontainers) — contador de janela fixa:
dentro do limite permite, no limiar seguinte recusa; janelas diferentes resetam; dois
`client_id` são independentes.
"""

import shutil
import subprocess
import time

import pytest
from redis.asyncio import Redis
from testcontainers.community.redis import RedisContainer

from adapters.cache.redis_rate_limiter import RedisRateLimiter

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


async def test_permite_ate_o_limite_e_recusa_no_seguinte(redis_client):
    limiter = RedisRateLimiter(redis_client, limit=2, window_seconds=60)

    assert await limiter.allow("cliente-1") is True
    assert await limiter.allow("cliente-1") is True
    assert await limiter.allow("cliente-1") is False


async def test_clientes_diferentes_tem_contadores_independentes(redis_client):
    limiter = RedisRateLimiter(redis_client, limit=1, window_seconds=60)

    assert await limiter.allow("cliente-a") is True
    assert await limiter.allow("cliente-b") is True
    assert await limiter.allow("cliente-a") is False


async def test_limiters_com_key_prefix_diferente_nao_compartilham_contador(redis_client):
    """Mesmo `client_id`, dois pontos de controle (geral vs. pesado) — `key_prefix`
    diferente garante contadores independentes, mesmo Redis (`docs/escalabilidade.md`:
    "limite separado, mais restritivo")."""
    general = RedisRateLimiter(
        redis_client, limit=1, window_seconds=60, key_prefix="ratelimit:request:"
    )
    heavy = RedisRateLimiter(
        redis_client, limit=1, window_seconds=60, key_prefix="ratelimit:heavy:"
    )

    assert await general.allow("cliente-1") is True
    assert await heavy.allow("cliente-1") is True  # não herdou o consumo do `general`


async def test_janela_expira_e_reseta_o_contador(redis_client):
    limiter = RedisRateLimiter(redis_client, limit=1, window_seconds=1)

    assert await limiter.allow("cliente-1") is True
    assert await limiter.allow("cliente-1") is False

    time.sleep(1.1)  # próxima janela

    assert await limiter.allow("cliente-1") is True
