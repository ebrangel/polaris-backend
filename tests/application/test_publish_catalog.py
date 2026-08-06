"""`PublishCatalog` — orquestração do pipeline de publicação (Marco 8), com fakes: sem
banco, sem Redis, sem inspector real. Segue o pseudocódigo de
`docs/pipeline-publicacao.md`: compila → compara hash → inspeciona → publica → invalida.
"""

import pytest

from adapters.catalog.yaml_loader import DEFAULT_SCHEMAS_DIR, load_schema_file
from application.use_cases.publish_catalog import PublishCatalog
from domain.errors import InvalidCatalogError
from fakes import InMemoryCatalogInvalidator, InMemoryCatalogRepository, StubDatasourceInspector


def _vendas_data() -> dict:
    return load_schema_file(DEFAULT_SCHEMAS_DIR / "vendas.yaml")


def _estoque_data() -> dict:
    return load_schema_file(DEFAULT_SCHEMAS_DIR / "estoque.yaml")


@pytest.fixture
def repository() -> InMemoryCatalogRepository:
    return InMemoryCatalogRepository()


@pytest.fixture
def invalidator() -> InMemoryCatalogInvalidator:
    return InMemoryCatalogInvalidator()


async def test_publica_um_schema_novo_e_invalida_o_cache(repository, invalidator):
    publish_catalog = PublishCatalog(
        repository=repository, inspectors={}, invalidator=invalidator
    )

    outcome = await publish_catalog(_estoque_data(), git_sha="abc123")

    assert outcome.published
    assert outcome.schema_name == "estoque"
    assert outcome.version is not None
    assert outcome.version.is_active
    assert invalidator.published == ["estoque"]


async def test_hash_identico_ao_ativo_nao_publica_nem_invalida(repository, invalidator):
    """"Comparar hash de conteúdo compilado ... pular a validação e a publicação
    quando idêntico" — `docs/pipeline-publicacao.md`."""
    publish_catalog = PublishCatalog(
        repository=repository, inspectors={}, invalidator=invalidator
    )
    data = _estoque_data()
    first = await publish_catalog(data, git_sha="abc123")
    assert first.published

    second = await publish_catalog(data, git_sha="def456")

    assert not second.published
    assert second.reason == "hash de conteúdo idêntico ao da versão ativa"
    assert second.version is None
    assert invalidator.published == ["estoque"]  # só a primeira publicação invalidou


async def test_hash_diferente_publica_uma_nova_versao(repository, invalidator):
    publish_catalog = PublishCatalog(
        repository=repository, inspectors={}, invalidator=invalidator
    )
    data = _estoque_data()
    first = await publish_catalog(data, git_sha="abc123")

    data["measures"]["valor_unitario"]["agg"] = "sum"  # muda o conteúdo compilado
    second = await publish_catalog(data, git_sha="def456")

    assert second.published
    assert second.version.content_hash != first.version.content_hash
    assert invalidator.published == ["estoque", "estoque"]


async def test_campo_faltando_no_datasource_aborta_antes_de_publicar(repository, invalidator):
    inspector = StubDatasourceInspector(missing={"estoque_atual_pg": ("filial",)})
    publish_catalog = PublishCatalog(
        repository=repository,
        inspectors={"env:APP_ESTOQUE_URL": inspector},
        invalidator=invalidator,
    )

    with pytest.raises(InvalidCatalogError, match="filial"):
        await publish_catalog(_estoque_data(), git_sha="abc123")

    assert await repository.get_active_version("estoque") is None
    assert invalidator.published == []


async def test_datasource_sem_inspector_e_reportado_mas_nao_falha(repository, invalidator):
    """Datasets Oracle não têm inspector real (mesma limitação do Marco 5) — o dataset
    entra em `uninspected_datasets`, e a publicação segue normalmente."""
    publish_catalog = PublishCatalog(
        repository=repository, inspectors={}, invalidator=invalidator
    )

    outcome = await publish_catalog(_vendas_data(), git_sha="abc123")

    assert outcome.published
    assert set(outcome.uninspected_datasets) == {"vendas_agregado_uf", "vendas_detalhado"}


async def test_apenas_datasets_sem_inspector_configurado_ficam_sem_inspecao(
    repository, invalidator
):
    inspector = StubDatasourceInspector()  # nunca acusa nada faltando
    publish_catalog = PublishCatalog(
        repository=repository,
        inspectors={"env:DW_VENDAS_PG_URL": inspector},
        invalidator=invalidator,
    )

    outcome = await publish_catalog(_vendas_data(), git_sha="abc123")

    assert outcome.published
    assert outcome.uninspected_datasets == ("vendas_detalhado",)  # só o Oracle
    assert len(inspector.calls) == 1
    assert inspector.calls[0].name == "vendas_agregado_uf"


async def test_catalogo_que_viola_o_modelo_logico_falha_na_compilacao(repository, invalidator):
    """`provides` referenciando um campo inexistente é invariante de domínio (Marco 1),
    não uma checagem deste use case — mas ainda precisa subir como `InvalidCatalogError`,
    sem tentar publicar nada."""
    publish_catalog = PublishCatalog(
        repository=repository, inspectors={}, invalidator=invalidator
    )
    data = _estoque_data()
    data["datasets"][0]["provides"]["dimensions"].append("regiao_inexistente")

    with pytest.raises(InvalidCatalogError):
        await publish_catalog(data, git_sha="abc123")

    assert invalidator.published == []


async def test_publish_outcome_carrega_o_git_sha_e_o_publicado_por(repository, invalidator):
    publish_catalog = PublishCatalog(
        repository=repository, inspectors={}, invalidator=invalidator
    )

    outcome = await publish_catalog(
        _estoque_data(), git_sha="deadbeef", published_by="pipeline-ci"
    )

    assert outcome.version.git_sha == "deadbeef"
    assert outcome.version.published_by == "pipeline-ci"
