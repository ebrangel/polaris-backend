"""`RedisCatalogInvalidator` + `listen_for_invalidation` contra um Redis real
(testcontainers) — o ciclo publisher/subscriber do tópico `catalog:invalidate`
(`docs/pipeline-publicacao.md`).
"""

import asyncio
import contextlib
import shutil
import subprocess

import pytest
from redis.asyncio import Redis
from testcontainers.community.redis import RedisContainer

from adapters.cache.redis_pubsub import RedisCatalogInvalidator, listen_for_invalidation

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
async def publisher_client(redis_url):
    client = Redis.from_url(redis_url, decode_responses=True)
    yield client
    await client.aclose()


@pytest.fixture
async def subscriber_client(redis_url):
    client = Redis.from_url(redis_url, decode_responses=True)
    yield client
    await client.aclose()


async def _wait_for_subscription(client: Redis, channel: str, timeout: float = 5.0) -> None:
    """`publish` antes de o assinante terminar `subscribe` some no vazio — Pub/Sub não
    tem buffer para quem ainda não assinou. Espera o canal aparecer nos assinantes do
    servidor antes de publicar, para o teste não ser instável."""
    async with asyncio.timeout(timeout):
        while True:
            channels = await client.pubsub_channels()
            if channel in channels:
                return
            await asyncio.sleep(0.05)


async def test_publish_dispara_o_callback_do_assinante_com_o_nome_do_schema(
    publisher_client, subscriber_client
):
    invalidator = RedisCatalogInvalidator(publisher_client)
    received: list[str] = []

    async def on_invalidate(schema_name: str) -> None:
        received.append(schema_name)

    task = asyncio.create_task(listen_for_invalidation(subscriber_client, on_invalidate))
    try:
        await _wait_for_subscription(publisher_client, "catalog:invalidate")
        await invalidator.publish("vendas")

        async with asyncio.timeout(5.0):
            while not received:
                await asyncio.sleep(0.05)

        assert received == ["vendas"]
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_varios_eventos_chegam_na_ordem_publicada(publisher_client, subscriber_client):
    invalidator = RedisCatalogInvalidator(publisher_client)
    received: list[str] = []

    async def on_invalidate(schema_name: str) -> None:
        received.append(schema_name)

    task = asyncio.create_task(listen_for_invalidation(subscriber_client, on_invalidate))
    try:
        await _wait_for_subscription(publisher_client, "catalog:invalidate")
        await invalidator.publish("vendas")
        await invalidator.publish("estoque")

        async with asyncio.timeout(5.0):
            while len(received) < 2:
                await asyncio.sleep(0.05)

        assert received == ["vendas", "estoque"]
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_cancelar_a_task_encerra_a_assinatura_sem_erro(publisher_client, subscriber_client):
    async def on_invalidate(schema_name: str) -> None:
        pass

    task = asyncio.create_task(listen_for_invalidation(subscriber_client, on_invalidate))
    await _wait_for_subscription(publisher_client, "catalog:invalidate")

    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert task.cancelled() or task.done()
