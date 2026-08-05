"""`ResolveDataset` — passo (2) do fluxo da seção 3: primeiro dataset que cobre a
requisição, em ordem de declaração. Casos derivados de `docs/catalogo-e-contrato-completo.md`.
"""

import dataclasses

import pytest
from fixtures import estoque_schema, eventos_schema, vendas_schema, vendas_schema_com_canal

from application.use_cases.resolve_dataset import ResolveDataset
from domain.errors import NoDatasetAvailableError, UnknownFieldError
from domain.models import Filter, FilterOperator, OrderBy, QueryRequest, SortDirection

resolve = ResolveDataset()


def test_pedido_so_com_sigla_uf_e_atendido_pelo_dataset_agregado():
    """O caso textual da seção 2.2: só `sigla_uf` é atendido por `vendas_agregado_uf`."""
    schema = vendas_schema()
    request = QueryRequest(
        schema="vendas",
        dimensions=("sigla_uf",),
        measures=("valor_total", "quantidade"),
    )

    assert resolve(schema, request).name == "vendas_agregado_uf"


def test_incluir_cargo_pula_para_o_dataset_detalhado():
    """"Se `dimensions` incluísse também `cargo`, o resolvedor pularia para
    `vendas_detalhado`" — seção 2.2."""
    schema = vendas_schema()
    request = QueryRequest(
        schema="vendas",
        dimensions=("sigla_uf", "cargo"),
        measures=("valor_total", "quantidade"),
    )

    assert resolve(schema, request).name == "vendas_detalhado"


def test_campo_usado_so_em_filtro_conta_para_a_cobertura():
    schema = vendas_schema()
    request = QueryRequest(
        schema="vendas",
        dimensions=("sigla_uf",),
        measures=("valor_total",),
        filters=(Filter(field="cargo", operator=FilterOperator.EQ, value="ANALISTA"),),
    )

    assert resolve(schema, request).name == "vendas_detalhado"


def test_campo_usado_so_em_ordenacao_conta_para_a_cobertura():
    schema = vendas_schema()
    request = QueryRequest(
        schema="vendas",
        dimensions=("sigla_uf",),
        measures=("valor_total",),
        order_by=(OrderBy(field="cargo", direction=SortDirection.ASC),),
    )

    assert resolve(schema, request).name == "vendas_detalhado"


def test_ordenacao_por_medida_nao_vira_exigencia_de_dimensao():
    """O pseudocódigo da seção 1.0 junta `order_fields` às dimensões, mas o exemplo da
    seção 2.2 ordena por `valor_total`, que é medida — não deve forçar `vendas_detalhado`."""
    schema = vendas_schema()
    request = QueryRequest(
        schema="vendas",
        dimensions=("sigla_uf",),
        measures=("valor_total",),
        order_by=(OrderBy(field="valor_total", direction=SortDirection.DESC),),
    )

    assert resolve(schema, request).name == "vendas_agregado_uf"


def test_ordem_de_declaracao_e_a_politica_de_otimizacao():
    """Trocar a ordem dos datasets no schema muda qual é escolhido, para a mesma
    requisição — a seção 1.0 é explícita: não há cálculo de "melhor" opção."""
    schema = vendas_schema()
    invertido = dataclasses.replace(schema, datasets=tuple(reversed(schema.datasets)))
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",), measures=("valor_total",))

    assert resolve(schema, request).name == "vendas_agregado_uf"
    assert resolve(invertido, request).name == "vendas_detalhado"


def test_no_dataset_available_com_o_exemplo_da_secao_2_5():
    """"Nenhum dataset do schema 'vendas' provê a combinação de campos: sigla_uf,
    cargo, canal." — seção 2.5, `type: no_dataset_available`, `status: 422`."""
    schema = vendas_schema_com_canal()
    request = QueryRequest(
        schema="vendas",
        dimensions=("sigla_uf", "cargo", "canal"),
        measures=("valor_total",),
    )

    with pytest.raises(NoDatasetAvailableError) as excinfo:
        resolve(schema, request)

    error = excinfo.value
    assert error.type == "no_dataset_available"
    assert error.fields == ("sigla_uf", "cargo", "canal", "valor_total")
    assert error.detail == (
        "Nenhum dataset do schema 'vendas' provê a combinação de campos: "
        "sigla_uf, cargo, canal, valor_total."
    )


def test_campo_desconhecido_no_modelo_logico_e_unknown_field_nao_no_dataset_available():
    """`canal` não existe em `vendas_schema()` (sem a fixture com canal) — o erro certo
    é `unknown_field`, levantado antes de qualquer varredura de datasets."""
    schema = vendas_schema()
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf", "canal"))

    with pytest.raises(UnknownFieldError):
        resolve(schema, request)


def test_dataset_elasticsearch_e_resolvido_sem_caminho_especial():
    schema = eventos_schema()
    request = QueryRequest(
        schema="eventos_navegacao",
        dimensions=("pais",),
        measures=("duracao_media",),
    )

    assert resolve(schema, request).name == "eventos_navegacao_es"


def test_dataset_plano_e_resolvido_sem_caminho_especial():
    schema = estoque_schema()
    request = QueryRequest(schema="estoque", dimensions=("filial",))

    assert resolve(schema, request).name == "estoque_atual_pg"
