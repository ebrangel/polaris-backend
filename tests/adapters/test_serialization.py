"""`request_to_dict`/`dict_to_request` e `result_to_dict`/`dict_to_result` — o
round-trip JSON-safe compartilhado por cache (Redis) e fila (`arq`). Puro, sem I/O.
"""

import json
from datetime import date, datetime, time
from decimal import Decimal
from uuid import UUID

from adapters.serialization import (
    dict_to_request,
    dict_to_result,
    jsonable,
    request_to_dict,
    result_to_dict,
)
from domain.models import (
    Column,
    DataType,
    Filter,
    FilterOperator,
    OrderBy,
    QueryRequest,
    QueryResult,
    QueryStatus,
    SortDirection,
)

#: O payload da seção 2.2, como objeto de domínio.
REQUEST_SECAO_2_2 = QueryRequest(
    schema="vendas",
    dimensions=("sigla_uf",),
    measures=("valor_total", "quantidade"),
    filters=(Filter(field="sigla_uf", operator=FilterOperator.IN, value=["SP", "RJ"]),),
    order_by=(OrderBy(field="valor_total", direction=SortDirection.DESC),),
    limit=100,
    offset=0,
)


# --- jsonable() ----------------------------------------------------------------------------


def test_jsonable_decimal_vira_string_sem_perder_precisao():
    assert jsonable(Decimal("458320.50")) == "458320.50"


def test_jsonable_datas_e_horarios_viram_iso():
    assert jsonable(date(2024, 1, 31)) == "2024-01-31"
    assert jsonable(datetime(2024, 1, 31, 10, 30)) == "2024-01-31T10:30:00"
    assert jsonable(time(10, 30)) == "10:30:00"


def test_jsonable_uuid_e_bytes():
    u = UUID("12345678-1234-5678-1234-567812345678")
    assert jsonable(u) == str(u)
    assert jsonable(b"abc") == "abc"


def test_jsonable_lista_e_tupla_recursivo():
    assert jsonable((Decimal("1.5"), "SP")) == ["1.5", "SP"]
    assert jsonable([date(2024, 1, 1)]) == ["2024-01-01"]


def test_jsonable_tipos_nativos_passam_direto():
    assert jsonable("SP") == "SP"
    assert jsonable(42) == 42
    assert jsonable(None) is None


# --- QueryRequest round-trip ---------------------------------------------------------------


def test_request_to_dict_e_json_dumps_compativel():
    """O ponto de existir: o dict precisa passar por `json.dumps` sem erro — é o que
    vai para o Redis e para o argumento de job do `arq`."""
    body = json.dumps(request_to_dict(REQUEST_SECAO_2_2))
    assert json.loads(body)["schema"] == "vendas"


def test_round_trip_preserva_o_query_id():
    """O `query_id` é o hash da requisição canônica (seção 3) — se o round-trip mudasse
    ordem de campos ou tipos, o hash mudaria junto."""
    restored = dict_to_request(request_to_dict(REQUEST_SECAO_2_2))

    assert restored == REQUEST_SECAO_2_2
    assert restored.query_id == REQUEST_SECAO_2_2.query_id


def test_round_trip_de_filtro_in_preserva_a_lista():
    restored = dict_to_request(request_to_dict(REQUEST_SECAO_2_2))

    assert restored.filters[0].operator is FilterOperator.IN
    assert restored.filters[0].value == ("SP", "RJ")


def test_round_trip_de_filtro_between():
    request = QueryRequest(
        schema="vendas",
        dimensions=("sigla_uf",),
        filters=(Filter(field="quantidade", operator=FilterOperator.BETWEEN, value=[10, 20]),),
    )

    restored = dict_to_request(request_to_dict(request))

    assert restored.filters[0].value == (10, 20)


def test_valor_de_filtro_data_perde_o_tipo_original_no_round_trip():
    """Limitação documentada em `serialization.py`: sem metadado de tipo, uma `date`
    volta como a mesma string ISO que já seria produzida na borda HTTP — mesma
    limitação de `POST /v1/query`, não uma regressão introduzida pela fila."""
    request = QueryRequest(
        schema="vendas",
        dimensions=("sigla_uf",),
        filters=(Filter(field="dia", operator=FilterOperator.EQ, value=date(2024, 1, 31)),),
    )

    restored = dict_to_request(request_to_dict(request))

    assert restored.filters[0].value == "2024-01-31"


def test_request_minima_sem_filtros_nem_ordenacao():
    request = QueryRequest(schema="vendas", dimensions=("sigla_uf",))

    restored = dict_to_request(request_to_dict(request))

    assert restored == request


# --- QueryResult round-trip -----------------------------------------------------------------


def test_round_trip_do_resultado_da_secao_2_3():
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

    restored = dict_to_result(result_to_dict(result))

    assert restored == result


def test_round_trip_preserva_decimal_como_string():
    result = QueryResult.completed(
        query_id="q_1",
        columns=(Column(field="valor_total", type=DataType.NUMBER),),
        rows=((Decimal("458320.50"),),),
        dataset_used="d",
    )

    restored_dict = result_to_dict(result)

    assert restored_dict["rows"] == [["458320.50"]]
    assert json.loads(json.dumps(restored_dict))  # serializável de ponta a ponta


def test_round_trip_processing():
    result = QueryResult.processing("q_9d31be")

    restored = dict_to_result(result_to_dict(result))

    assert restored == result
    assert restored.status is QueryStatus.PROCESSING


def test_round_trip_failed():
    result = QueryResult.failed("q_9d31be", error="query_timeout")

    restored = dict_to_result(result_to_dict(result))

    assert restored == result
    assert restored.error == "query_timeout"


def test_coluna_sem_format_preserva_none():
    result = QueryResult.completed(
        query_id="q_1",
        columns=(Column(field="quantidade", type=DataType.NUMBER),),
        rows=((1,),),
        dataset_used="d",
    )

    body = result_to_dict(result)

    assert body["columns"][0]["format"] is None
    assert dict_to_result(body) == result
