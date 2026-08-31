"""Headers da resposta CSV (seção 2.3a) — a parte HTTP do presenter.

A escrita do CSV em si é de `tests/adapters/test_csv_format.py`, compartilhada com o
worker.
"""

from adapters.api.csv_presenter import csv_filename, csv_headers
from domain.models import Column, DataType, QueryResult


def test_nome_do_arquivo_vem_do_query_id():
    assert csv_filename("q_8f2a1c") == "q_8f2a1c.csv"


def test_headers_carregam_o_meta_da_secao_2_3():
    result = QueryResult.completed(
        query_id="q_8f2a1c",
        columns=(Column(field="sigla_uf", type=DataType.STRING),),
        rows=(("SP",),),
        dataset_used="vendas_agregado_uf",
        cached=True,
        execution_ms=12,
    )

    assert csv_headers(result) == {
        "Content-Disposition": 'attachment; filename="q_8f2a1c.csv"',
        "X-Query-Id": "q_8f2a1c",
        "X-Row-Count": "1",
        "X-Cached": "true",
        "X-Execution-Ms": "12",
        "X-Dataset-Used": "vendas_agregado_uf",
    }
