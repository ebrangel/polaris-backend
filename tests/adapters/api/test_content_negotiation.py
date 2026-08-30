"""Negociação do formato de saída (seção 2.3a) — módulo puro, sem app.

`?format=` explícito > `Accept` > JSON por omissão.
"""

import pytest

from adapters.api.content_negotiation import (
    OutputFormat,
    UnsupportedFormatError,
    resolve_output_format,
)


def test_sem_format_e_sem_accept_e_json():
    assert resolve_output_format(None, None) is OutputFormat.JSON


@pytest.mark.parametrize("valor", ["csv", "CSV", " csv "])
def test_format_csv_em_qualquer_caixa(valor):
    assert resolve_output_format(valor, None) is OutputFormat.CSV


def test_format_vazio_cai_no_accept():
    """`?format=` sem valor é o mesmo que não pedir formato nenhum."""
    assert resolve_output_format("", "text/csv") is OutputFormat.CSV


def test_format_desconhecido_e_erro():
    with pytest.raises(UnsupportedFormatError) as exc_info:
        resolve_output_format("parquet", None)

    assert exc_info.value.requested == "parquet"
    assert set(exc_info.value.supported) == {"json", "csv"}


def test_format_explicito_vence_o_accept():
    """O link de dashboard manda `?format=csv`; o navegador manda um `Accept` que não
    cita CSV. O parâmetro é que decide."""
    accept = "text/html,application/xhtml+xml,*/*;q=0.8"

    assert resolve_output_format("csv", accept) is OutputFormat.CSV
    assert resolve_output_format("json", "text/csv") is OutputFormat.JSON


def test_accept_text_csv():
    assert resolve_output_format(None, "text/csv") is OutputFormat.CSV


def test_accept_com_parametros_do_media_type():
    assert resolve_output_format(None, "text/csv; charset=utf-8") is OutputFormat.CSV


def test_accept_de_navegador_cai_em_json():
    """`*/*;q=0.8` é o primeiro item aceitável que a API produz — e vale JSON."""
    accept = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,*/*;q=0.8"

    assert resolve_output_format(None, accept) is OutputFormat.JSON


def test_accept_de_curl_cai_em_json():
    assert resolve_output_format(None, "*/*") is OutputFormat.JSON


def test_accept_respeita_a_ordem_de_preferencia_por_q():
    assert resolve_output_format(None, "application/json;q=0.2,text/csv;q=0.9") is (
        OutputFormat.CSV
    )
    assert resolve_output_format(None, "text/csv;q=0.2,application/json;q=0.9") is (
        OutputFormat.JSON
    )


def test_accept_ignora_media_range_com_q_zero():
    assert resolve_output_format(None, "text/csv;q=0,*/*") is OutputFormat.JSON


def test_accept_desconhecido_nao_recusa_a_requisicao():
    """Assimetria proposital: `Accept` inservível cai em JSON em vez de 406."""
    assert resolve_output_format(None, "application/pdf") is OutputFormat.JSON


def test_accept_text_generico_e_csv():
    assert resolve_output_format(None, "text/*") is OutputFormat.CSV
