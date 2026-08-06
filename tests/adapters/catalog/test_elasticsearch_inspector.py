"""`ElasticsearchInspector` contra um Elasticsearch real (testcontainers).

Imagem 9.x para casar com o major do client instalado — mesma decisão registrada em
`tests/adapters/executors/test_elasticsearch_integration.py` (Marco 5).
"""

import asyncio
import shutil
import subprocess

import pytest
from elasticsearch import AsyncElasticsearch
from fixtures import eventos_navegacao_es
from testcontainers.community.elasticsearch import ElasticSearchContainer

from adapters.catalog.elasticsearch_inspector import ElasticsearchInspector
from domain.models import FieldMapping, IndexModel

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
    await client.close()


@pytest.fixture(scope="module")
def es_url():
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


@pytest.fixture
def inspector(es_client) -> ElasticsearchInspector:
    return ElasticsearchInspector(es_client)


async def test_indice_com_todos_os_campos_do_mapping_nao_acusa_nada(inspector):
    dataset = eventos_navegacao_es()

    assert await inspector.missing_fields(dataset) == ()


async def test_indice_com_campo_renomeado_acusa_exatamente_o_campo_faltante(inspector):
    dataset = eventos_navegacao_es()
    modelo_quebrado = IndexModel(
        name=dataset.model.name,
        mapping={
            **dataset.model.mapping,
            "pais": FieldMapping(field="campo_que_nao_existe", es_type="keyword"),
        },
    )
    dataset_quebrado = dataset.__class__(
        name=dataset.name,
        datasource=dataset.datasource,
        provides=dataset.provides,
        model=modelo_quebrado,
    )

    missing = await inspector.missing_fields(dataset_quebrado)

    assert missing == ("pais",)


async def test_indice_inexistente_reporta_todos_os_campos_como_faltantes(inspector):
    dataset = eventos_navegacao_es()
    modelo_outro_indice = IndexModel(name="indice-que-nao-existe", mapping=dataset.model.mapping)
    dataset_outro_indice = dataset.__class__(
        name=dataset.name,
        datasource=dataset.datasource,
        provides=dataset.provides,
        model=modelo_outro_indice,
    )

    missing = await inspector.missing_fields(dataset_outro_indice)

    assert set(missing) == set(dataset.model.mapping)
