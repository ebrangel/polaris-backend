"""`QueryResult` — respostas síncrona (seção 2.3) e assíncrona (seção 2.4)."""

import pytest

from domain.models import Column, DataType, QueryResult, QueryStatus, ResultMeta


def test_resposta_sincrona_da_secao_2_3():
    result = QueryResult.completed(
        query_id="q_8f2a1c",
        columns=(
            Column(field="sigla_uf", type=DataType.STRING),
            Column(field="valor_total", type=DataType.NUMBER, format="currency"),
            Column(field="quantidade", type=DataType.NUMBER),
        ),
        rows=(("SP", 458320.50, 1204), ("RJ", 212904.10, 588)),
        dataset_used="vendas_agregado_uf",
        cached=True,
        execution_ms=12,
    )

    assert result.status is QueryStatus.COMPLETED
    assert result.meta == ResultMeta(
        row_count=2, cached=True, execution_ms=12, dataset_used="vendas_agregado_uf"
    )
    assert result.rows[0] == ("SP", 458320.50, 1204)


def test_resposta_assincrona_da_secao_2_4():
    result = QueryResult.processing("q_9d31be")

    assert result.query_id == "q_9d31be"
    assert result.status is QueryStatus.PROCESSING
    assert result.rows == ()
    assert result.columns == ()
    assert result.meta is None


def test_resultado_com_falha():
    result = QueryResult.failed("q_9d31be", error="query_timeout")

    assert result.status is QueryStatus.FAILED
    assert result.error == "query_timeout"


def test_linha_com_largura_diferente_das_colunas():
    with pytest.raises(ValueError, match="colunas"):
        QueryResult.completed(
            query_id="q_8f2a1c",
            columns=(Column(field="sigla_uf", type=DataType.STRING),),
            rows=(("SP", 458320.50),),
            dataset_used="vendas_agregado_uf",
        )


def test_row_count_precisa_bater_com_as_linhas():
    with pytest.raises(ValueError, match="row_count"):
        QueryResult(
            query_id="q_8f2a1c",
            status=QueryStatus.COMPLETED,
            columns=(Column(field="sigla_uf", type=DataType.STRING),),
            rows=(("SP",), ("RJ",)),
            meta=ResultMeta(
                row_count=99, cached=False, execution_ms=1, dataset_used="qualquer"
            ),
        )


def test_resultado_concluido_precisa_de_meta():
    with pytest.raises(ValueError, match="meta"):
        QueryResult(
            query_id="q_8f2a1c",
            status=QueryStatus.COMPLETED,
            columns=(Column(field="sigla_uf", type=DataType.STRING),),
            rows=(("SP",),),
        )


def test_resultado_em_processamento_nao_carrega_linhas():
    with pytest.raises(ValueError, match="não carrega"):
        QueryResult(
            query_id="q_9d31be",
            status=QueryStatus.PROCESSING,
            columns=(Column(field="sigla_uf", type=DataType.STRING),),
            rows=(("SP",),),
        )


def test_resultado_com_falha_precisa_informar_o_erro():
    with pytest.raises(ValueError, match="não informa o erro"):
        QueryResult(query_id="q_9d31be", status=QueryStatus.FAILED)


def test_resultado_vazio_e_valido():
    result = QueryResult.completed(
        query_id="q_000000",
        columns=(Column(field="sigla_uf", type=DataType.STRING),),
        rows=(),
        dataset_used="vendas_agregado_uf",
    )

    assert result.meta.row_count == 0
