"""`QueryResult` → CSV da RFC 4180 (seção 2.3a) — escritor puro, sem app.

O mesmo `csv_lines` alimenta a resposta HTTP e o arquivo que o worker grava (seção
2.4a), então estes casos valem para os dois caminhos.
"""

from datetime import date
from decimal import Decimal

from adapters.csv_format import csv_lines
from domain.models import Column, DataType, QueryResult


def _result(columns, rows, **kwargs) -> QueryResult:
    return QueryResult.completed(
        query_id=kwargs.pop("query_id", "q_8f2a1c"),
        columns=columns,
        rows=rows,
        dataset_used=kwargs.pop("dataset_used", "vendas_agregado_uf"),
        **kwargs,
    )


def _texto(result, **kwargs) -> str:
    return "".join(csv_lines(result, **kwargs))


def test_cabecalho_e_linhas_da_secao_2_3():
    result = _result(
        columns=(
            Column(field="sigla_uf", type=DataType.STRING),
            Column(field="valor_total", type=DataType.NUMBER, format="currency"),
            Column(field="quantidade", type=DataType.NUMBER),
        ),
        rows=(("SP", 458320.50, 1204), ("RJ", 212904.10, 588)),
    )

    assert _texto(result) == (
        "sigla_uf,valor_total,quantidade\r\n"
        "SP,458320.5,1204\r\n"
        "RJ,212904.1,588\r\n"
    )


def test_resultado_sem_linhas_ainda_traz_o_cabecalho():
    result = _result(columns=(Column(field="sigla_uf", type=DataType.STRING),), rows=())

    assert _texto(result) == "sigla_uf\r\n"


def test_format_da_coluna_nao_e_aplicado():
    """`format: currency` é dica de apresentação — o CSV leva o valor cru."""
    result = _result(
        columns=(Column(field="valor_total", type=DataType.NUMBER, format="currency"),),
        rows=((Decimal("458320.50"),),),
    )

    assert _texto(result) == "valor_total\r\n458320.50\r\n"


def test_decimal_preserva_a_precisao():
    """Mesma decisão de `jsonable()`: texto exato, nunca `float`."""
    result = _result(
        columns=(Column(field="valor_total", type=DataType.NUMBER),),
        rows=((Decimal("0.1000000000000000055511151231257827"),),),
    )

    assert "0.1000000000000000055511151231257827" in _texto(result)


def test_none_vira_campo_vazio():
    result = _result(
        columns=(
            Column(field="sigla_uf", type=DataType.STRING),
            Column(field="valor_total", type=DataType.NUMBER),
        ),
        rows=((None, 10), ("SP", None)),
    )

    assert _texto(result) == "sigla_uf,valor_total\r\n,10\r\nSP,\r\n"


def test_booleano_sai_como_no_json():
    result = _result(
        columns=(Column(field="ativo", type=DataType.BOOLEAN),),
        rows=((True,), (False,)),
    )

    assert _texto(result) == "ativo\r\ntrue\r\nfalse\r\n"


def test_data_em_iso_8601():
    result = _result(
        columns=(Column(field="data_venda", type=DataType.DATE),),
        rows=((date(2024, 3, 15),),),
    )

    assert _texto(result) == "data_venda\r\n2024-03-15\r\n"


def test_lista_vira_json_na_celula():
    """Valor multivalorado (Elasticsearch) não tem como virar duas colunas."""
    result = _result(
        columns=(Column(field="tags", type=DataType.STRING),),
        rows=((["promoção", "online"],),),
    )

    assert _texto(result) == 'tags\r\n"[""promoção"", ""online""]"\r\n'


def test_quoting_da_rfc_4180():
    """Vírgula, aspas e quebra de linha dentro do valor — o caso que uma concatenação
    manual com `join` erraria."""
    result = _result(
        columns=(Column(field="descricao", type=DataType.STRING),),
        rows=(("Sao Paulo, SP",), ('aspas " no meio',), ("duas\nlinhas",)),
    )

    assert _texto(result) == (
        "descricao\r\n"
        '"Sao Paulo, SP"\r\n'
        '"aspas "" no meio"\r\n'
        '"duas\nlinhas"\r\n'
    )


def test_delimitador_alternativo_para_excel():
    result = _result(
        columns=(
            Column(field="sigla_uf", type=DataType.STRING),
            Column(field="valor_total", type=DataType.NUMBER),
        ),
        rows=(("SP", 458320.50),),
    )

    assert _texto(result, delimiter=";") == (
        "sigla_uf;valor_total\r\nSP;458320.5\r\n"
    )


def test_linhas_saem_uma_a_uma():
    """É gerador, não string única: quem consome escreve no socket ou no arquivo sem
    montar o CSV inteiro em memória."""
    result = _result(
        columns=(Column(field="sigla_uf", type=DataType.STRING),),
        rows=(("SP",), ("RJ",), ("MG",)),
    )

    assert list(csv_lines(result)) == [
        "sigla_uf\r\n",
        "SP\r\n",
        "RJ\r\n",
        "MG\r\n",
    ]
