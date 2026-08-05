"""`Dataset.covers()` e `Schema.split_fields()` — domínio puro, sem resolução em ordem.

A varredura em ordem de declaração (o `select_dataset` da seção 1.0 propriamente dito)
é testada em `tests/application/test_resolve_dataset.py`, contra o use case `ResolveDataset`.
"""

from fixtures import estoque_schema, eventos_schema, vendas_schema

from domain.models import OrderBy, QueryRequest, SortDirection


def test_dataset_agregado_nao_cobre_cargo():
    """`vendas_agregado_uf` (primeiro dataset da seção 1.0) não provê `cargo`."""
    dataset = vendas_schema().datasets[0]

    assert dataset.covers({"sigla_uf"}, {"valor_total", "quantidade"})
    assert not dataset.covers({"sigla_uf", "cargo"}, {"valor_total"})


def test_dataset_detalhado_cobre_sigla_uf_e_cargo():
    """`vendas_detalhado` (segundo dataset da seção 1.0) provê os dois."""
    dataset = vendas_schema().datasets[1]

    assert dataset.covers({"sigla_uf", "cargo"}, {"valor_total", "quantidade"})


def test_nenhum_dataset_cobre_a_combinacao():
    schema = vendas_schema()

    assert not any(
        dataset.covers({"sigla_uf", "cargo", "canal"}, {"valor_total"})
        for dataset in schema.datasets
    )


def test_dataset_elasticsearch_cobre_seu_modelo_plano():
    dataset = eventos_schema().datasets[0]

    assert dataset.covers({"pais", "dispositivo"}, {"duracao_media", "total_eventos"})
    assert not dataset.covers({"pais", "navegador"}, set())


def test_dataset_plano_cobre_seu_modelo():
    dataset = estoque_schema().datasets[0]

    assert dataset.covers({"filial"}, {"quantidade_disponivel"})


def test_split_fields_classifica_campo_de_filtro_e_ordenacao():
    """Campos usados só em filtro/ordenação também contam para a cobertura (seção 1.0)
    — `split_fields()` é quem separa `referenced_fields()` em dimensões e medidas."""
    schema = vendas_schema()
    request = QueryRequest(
        schema="vendas",
        dimensions=("sigla_uf",),
        measures=("valor_total",),
        order_by=(OrderBy(field="cargo", direction=SortDirection.ASC),),
    )

    dimensions, measures = schema.split_fields(request.referenced_fields())

    assert dimensions == {"sigla_uf", "cargo"}
    assert measures == {"valor_total"}


def test_split_fields_nao_trata_medida_em_order_by_como_dimensao():
    """O pseudocódigo da seção 1.0 junta `order_fields` às dimensões, mas o exemplo da
    seção 2.2 ordena por `valor_total`, que é medida — `split_fields` classifica certo."""
    schema = vendas_schema()
    request = QueryRequest(
        schema="vendas",
        dimensions=("sigla_uf",),
        measures=("valor_total",),
        order_by=(OrderBy(field="valor_total", direction=SortDirection.DESC),),
    )

    dimensions, measures = schema.split_fields(request.referenced_fields())

    assert dimensions == {"sigla_uf"}
    assert measures == {"valor_total"}
