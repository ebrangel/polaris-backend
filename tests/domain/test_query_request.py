"""`QueryRequest` — o objeto para onde POST e GET convergem (seções 2.2 e 2.2a)."""

import pytest
from fixtures import vendas_schema

from domain.errors import (
    ForbiddenMeasureError,
    InvalidFilterError,
    UnknownFieldError,
    UnknownSchemaError,
)
from domain.models import (
    DataType,
    Dimension,
    Filter,
    FilterOperator,
    OrderBy,
    QueryRequest,
    Schema,
    SortDirection,
)

#: Corpo de requisição da seção 2.2, tal como documentado.
PAYLOAD_SECAO_2_2 = {
    "schema": "vendas",
    "dimensions": ["sigla_uf"],
    "measures": ["valor_total", "quantidade"],
    "filters": [{"field": "sigla_uf", "operator": "in", "value": ["SP", "RJ"]}],
    "order_by": [{"field": "valor_total", "direction": "desc"}],
    "limit": 100,
    "offset": 0,
}


def request_from_payload(payload: dict) -> QueryRequest:
    """Tradução direta do JSON do contrato — o parsing real mora no adapter (Marco 6)."""
    return QueryRequest(
        schema=payload["schema"],
        dimensions=tuple(payload.get("dimensions", ())),
        measures=tuple(payload.get("measures", ())),
        filters=tuple(
            Filter(
                field=f["field"],
                operator=FilterOperator(f["operator"]),
                value=f["value"],
            )
            for f in payload.get("filters", ())
        ),
        order_by=tuple(
            OrderBy(field=o["field"], direction=SortDirection(o["direction"]))
            for o in payload.get("order_by", ())
        ),
        limit=payload.get("limit"),
        offset=payload.get("offset", 0),
    )


def test_payload_da_secao_2_2_vira_uma_requisicao_de_dominio():
    request = request_from_payload(PAYLOAD_SECAO_2_2)

    assert request.schema == "vendas"
    assert request.dimensions == ("sigla_uf",)
    assert request.measures == ("valor_total", "quantidade")
    assert request.filters == (
        Filter(field="sigla_uf", operator=FilterOperator.IN, value=("SP", "RJ")),
    )
    assert request.order_by == (
        OrderBy(field="valor_total", direction=SortDirection.DESC),
    )
    assert request.limit == 100
    assert request.offset == 0


def test_get_e_post_convergem_para_a_mesma_requisicao():
    """A opção A da seção 2.2a é o mesmo JSON do POST, url-encoded."""
    import json
    from urllib.parse import parse_qs, urlencode

    querystring = urlencode({"query": json.dumps(PAYLOAD_SECAO_2_2)})
    decoded = json.loads(parse_qs(querystring)["query"][0])

    assert request_from_payload(decoded) == request_from_payload(PAYLOAD_SECAO_2_2)


def test_campos_referenciados_incluem_filtros_e_ordenacao():
    request = request_from_payload(PAYLOAD_SECAO_2_2)

    assert request.referenced_fields() == {"sigla_uf", "valor_total", "quantidade"}


# --- Invariantes da requisição ---------------------------------------------------------


def test_requisicao_precisa_de_ao_menos_um_campo():
    with pytest.raises(UnknownFieldError):
        QueryRequest(schema="vendas")


def test_requisicao_precisa_de_schema():
    with pytest.raises(UnknownSchemaError):
        QueryRequest(schema="", dimensions=("sigla_uf",))


@pytest.mark.parametrize("limit, offset", [(-1, 0), (None, -5)])
def test_paginacao_negativa_e_rejeitada(limit, offset):
    with pytest.raises(InvalidFilterError):
        QueryRequest(
            schema="vendas", dimensions=("sigla_uf",), limit=limit, offset=offset
        )


def test_operador_in_exige_lista_nao_vazia():
    with pytest.raises(InvalidFilterError, match="lista não vazia"):
        Filter(field="sigla_uf", operator=FilterOperator.IN, value=[])

    with pytest.raises(InvalidFilterError, match="lista não vazia"):
        Filter(field="sigla_uf", operator=FilterOperator.IN, value="SP")


def test_operador_between_exige_exatamente_dois_valores():
    with pytest.raises(InvalidFilterError, match="dois"):
        Filter(field="quantidade", operator=FilterOperator.BETWEEN, value=[1, 2, 3])


def test_operador_escalar_rejeita_lista():
    with pytest.raises(InvalidFilterError, match="escalar"):
        Filter(field="sigla_uf", operator=FilterOperator.EQ, value=["SP", "RJ"])


# --- Validação contra o modelo lógico --------------------------------------------------


def test_requisicao_valida_passa():
    vendas_schema().validate_request(request_from_payload(PAYLOAD_SECAO_2_2))


def test_campo_inexistente_no_schema():
    request = QueryRequest(
        schema="vendas", dimensions=("sigla_uf", "canal"), measures=("valor_total",)
    )

    with pytest.raises(UnknownFieldError) as excinfo:
        vendas_schema().validate_request(request)

    assert excinfo.value.fields == ("canal",)
    assert excinfo.value.as_problem()["type"] == "unknown_field"


def test_medida_pedida_como_dimensao():
    request = QueryRequest(schema="vendas", dimensions=("valor_total",))

    with pytest.raises(UnknownFieldError, match="são medidas"):
        vendas_schema().validate_request(request)


def test_dimensao_pedida_como_medida():
    request = QueryRequest(schema="vendas", measures=("sigla_uf",))

    with pytest.raises(UnknownFieldError, match="são dimensões"):
        vendas_schema().validate_request(request)


def test_requisicao_para_outro_schema():
    request = QueryRequest(schema="estoque", dimensions=("filial",))

    with pytest.raises(UnknownSchemaError):
        vendas_schema().validate_request(request)


def test_contains_so_vale_para_string():
    schema_com_numero = Schema(
        name="vendas",
        version=1,
        dimensions={"ano": Dimension(name="ano", type=DataType.NUMBER)},
        measures={},
    )
    request = QueryRequest(
        schema="vendas",
        dimensions=("ano",),
        filters=(Filter(field="ano", operator=FilterOperator.CONTAINS, value="20"),),
    )

    with pytest.raises(InvalidFilterError) as excinfo:
        schema_com_numero.validate_request(request)

    assert "contains" in excinfo.value.detail
    assert excinfo.value.fields == ("ano",)


def test_dimensao_nao_filtravel():
    schema = Schema(
        name="vendas",
        version=1,
        dimensions={"sigla_uf": Dimension(name="sigla_uf", filterable=False)},
        measures={},
    )
    request = QueryRequest(
        schema="vendas",
        dimensions=("sigla_uf",),
        filters=(Filter(field="sigla_uf", operator=FilterOperator.EQ, value="SP"),),
    )

    with pytest.raises(InvalidFilterError, match="não é filtrável"):
        schema.validate_request(request)


def test_filtro_sobre_medida_e_recusado():
    request = QueryRequest(
        schema="vendas",
        dimensions=("sigla_uf",),
        measures=("valor_total",),
        filters=(Filter(field="valor_total", operator=FilterOperator.GT, value=100),),
    )

    with pytest.raises(InvalidFilterError, match="filtrar por dimensões"):
        vendas_schema().validate_request(request)


# --- Controle de acesso ----------------------------------------------------------------


def test_role_com_acesso_a_todas_as_medidas():
    vendas_schema().authorize(request_from_payload(PAYLOAD_SECAO_2_2), ["financeiro"])


def test_role_sem_acesso_a_medida():
    request = request_from_payload(PAYLOAD_SECAO_2_2)

    with pytest.raises(ForbiddenMeasureError) as excinfo:
        vendas_schema().authorize(request, ["comercial"])

    assert excinfo.value.fields == ("quantidade", "valor_total")
    assert excinfo.value.as_problem()["type"] == "forbidden_measure"


# --- Paginação e colunas ---------------------------------------------------------------


def test_limite_maximo_do_schema():
    base = vendas_schema()
    schema = Schema(
        name=base.name,
        version=base.version,
        dimensions=base.dimensions,
        measures=base.measures,
        datasets=base.datasets,
        max_limit=500,
    )

    assert schema.effective_limit(100) == 100
    assert schema.effective_limit(10_000) == 500
    assert schema.effective_limit(None) == 500
    assert base.effective_limit(10_000) == 10_000


def test_colunas_da_resposta_seguem_a_ordem_pedida():
    """As colunas da seção 2.3 saem do modelo lógico: medidas são `number`."""
    request = request_from_payload(PAYLOAD_SECAO_2_2)
    columns = vendas_schema().columns_for(request)

    assert [(c.field, c.type.value, c.format) for c in columns] == [
        ("sigla_uf", "string", None),
        ("valor_total", "number", "currency"),
        ("quantidade", "number", None),
    ]


# --- query_id / fingerprint (seção 3) --------------------------------------------------


def test_query_id_tem_o_formato_do_contrato():
    query_id = request_from_payload(PAYLOAD_SECAO_2_2).query_id

    assert query_id.startswith("q_")
    assert len(query_id) == 8
    assert all(c in "0123456789abcdef" for c in query_id[2:])


def test_fingerprint_e_deterministico():
    assert (
        request_from_payload(PAYLOAD_SECAO_2_2).fingerprint()
        == request_from_payload(PAYLOAD_SECAO_2_2).fingerprint()
    )


def test_requisicoes_equivalentes_tem_o_mesmo_query_id():
    """Filtros formam uma conjunção e valores de `in` são um conjunto: reordená-los não
    muda o resultado, logo não pode mudar a chave de cache."""
    a = QueryRequest(
        schema="vendas",
        dimensions=("sigla_uf",),
        measures=("valor_total",),
        filters=(
            Filter(field="sigla_uf", operator=FilterOperator.IN, value=["SP", "RJ"]),
            Filter(field="cargo", operator=FilterOperator.EQ, value="ANALISTA"),
        ),
    )
    b = QueryRequest(
        schema="vendas",
        dimensions=("sigla_uf",),
        measures=("valor_total",),
        filters=(
            Filter(field="cargo", operator=FilterOperator.EQ, value="ANALISTA"),
            Filter(field="sigla_uf", operator=FilterOperator.IN, value=["RJ", "SP"]),
        ),
    )

    assert a.query_id == b.query_id


def test_ordem_das_dimensoes_muda_o_query_id():
    """A ordem das dimensões muda a ordem das colunas na resposta — é outra consulta."""
    a = QueryRequest(schema="vendas", dimensions=("sigla_uf", "cargo"))
    b = QueryRequest(schema="vendas", dimensions=("cargo", "sigla_uf"))

    assert a.query_id != b.query_id


def test_paginacao_diferente_muda_o_query_id():
    a = QueryRequest(schema="vendas", dimensions=("sigla_uf",), limit=100, offset=0)
    b = QueryRequest(schema="vendas", dimensions=("sigla_uf",), limit=100, offset=100)

    assert a.query_id != b.query_id


def test_ordenacao_diferente_muda_o_query_id():
    a = QueryRequest(
        schema="vendas",
        dimensions=("sigla_uf",),
        measures=("valor_total",),
        order_by=(OrderBy(field="valor_total", direction=SortDirection.DESC),),
    )
    b = QueryRequest(
        schema="vendas",
        dimensions=("sigla_uf",),
        measures=("valor_total",),
        order_by=(OrderBy(field="valor_total", direction=SortDirection.ASC),),
    )

    assert a.query_id != b.query_id
