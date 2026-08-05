"""`ElasticsearchQueryExecutor` contra um Elasticsearch real (testcontainers) — prova
que a Query DSL compilada no Marco 5 (ver `test_elasticsearch_dsl.py`) de fato executa
e devolve agregações corretas.

Imagem 9.x para casar com o major do client instalado (`elasticsearch` 9.x): clientes e
servidores Elasticsearch de majors diferentes recusam a negociação de
Accept/Content-Type (`compatible-with=9` rejeitado por um servidor 8.x).
"""

import asyncio
import shutil
import subprocess

import pytest
from elasticsearch import AsyncElasticsearch
from fixtures import eventos_navegacao_es, eventos_schema
from testcontainers.community.elasticsearch import ElasticSearchContainer

from adapters.executors.elasticsearch_executor import ElasticsearchQueryExecutor
from domain.errors import QueryTimeoutError
from domain.models import Filter, FilterOperator, QueryRequest, QueryStatus

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


_INDEX = "eventos-navegacao"
_DOCUMENTS = [
    {"pais": "BR", "dispositivo": "mobile", "duracao_sessao": 100},
    {"pais": "BR", "dispositivo": "desktop", "duracao_sessao": 300},
    {"pais": "AR", "dispositivo": "mobile", "duracao_sessao": 50},
]


async def _seed(url: str) -> None:
    client = AsyncElasticsearch(hosts=[url])
    await client.indices.create(
        index=_INDEX,
        mappings={
            "properties": {
                "pais": {"type": "keyword"},
                "dispositivo": {"type": "keyword"},
                "duracao_sessao": {"type": "integer"},
            }
        },
    )
    for doc in _DOCUMENTS:
        await client.index(index=_INDEX, document=doc)
    await client.indices.refresh(index=_INDEX)
    await client.close()


@pytest.fixture(scope="module")
def es_url():
    """Container único para o módulo; a semente é indexada uma vez via `asyncio.run`
    (loop próprio, fechado em seguida) — cada teste cria seu próprio client
    (`es_client`), preso ao event loop daquele teste."""
    container = ElasticSearchContainer(
        image="docker.elastic.co/elasticsearch/elasticsearch:9.0.0"
    )
    container.with_env("xpack.security.enabled", "false")
    container.with_env("discovery.type", "single-node")
    container.start()
    try:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(9200)
        url = f"http://{host}:{port}"
        asyncio.run(_seed(url))
        yield url
    finally:
        container.stop()


@pytest.fixture
async def es_client(es_url):
    client = AsyncElasticsearch(hosts=[es_url])
    yield client
    await client.close()


async def test_exemplo_da_secao_1_1_com_as_duas_dimensoes(es_client):
    schema = eventos_schema()
    dataset = eventos_navegacao_es()
    executor = ElasticsearchQueryExecutor(client=es_client)
    request = QueryRequest(
        schema="eventos_navegacao",
        dimensions=("pais", "dispositivo"),
        measures=("duracao_media", "total_eventos"),
    )
    columns = schema.columns_for(request)

    result = await executor.execute(dataset, request, columns)

    assert result.status is QueryStatus.COMPLETED
    assert set(result.rows) == {
        ("BR", "mobile", 100.0, 1),
        ("BR", "desktop", 300.0, 1),
        ("AR", "mobile", 50.0, 1),
    }
    assert result.meta.dataset_used == "eventos_navegacao_es"


async def test_filtro_eq_sobre_dimensao(es_client):
    schema = eventos_schema()
    dataset = eventos_navegacao_es()
    executor = ElasticsearchQueryExecutor(client=es_client)
    request = QueryRequest(
        schema="eventos_navegacao",
        dimensions=("pais",),
        measures=("total_eventos",),
        filters=(Filter(field="pais", operator=FilterOperator.EQ, value="BR"),),
    )
    columns = schema.columns_for(request)

    result = await executor.execute(dataset, request, columns)

    assert result.rows == (("BR", 2),)


async def test_zero_dimensoes_agrega_o_indice_inteiro(es_client):
    schema = eventos_schema()
    dataset = eventos_navegacao_es()
    executor = ElasticsearchQueryExecutor(client=es_client)
    request = QueryRequest(schema="eventos_navegacao", measures=("total_eventos",))
    columns = schema.columns_for(request)

    result = await executor.execute(dataset, request, columns)

    assert result.rows == ((3,),)


async def test_timeout_real_vira_query_timeout_error(es_client):
    schema = eventos_schema()
    dataset = eventos_navegacao_es()
    executor = ElasticsearchQueryExecutor(client=es_client, light_timeout_seconds=0.001)
    request = QueryRequest(
        schema="eventos_navegacao", dimensions=("pais", "dispositivo"), measures=("duracao_media",)
    )
    columns = schema.columns_for(request)

    with pytest.raises(QueryTimeoutError):
        await executor.execute(dataset, request, columns)
