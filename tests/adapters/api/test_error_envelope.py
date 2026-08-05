"""Formato único de erro da seção 2.5, no estilo `application/problem+json`.

O mapa `type → status` vive no adapter de API (decisão do Marco 2, quando `DomainError`
foi criado sem campo `status`). A documentação só fixa `422` para `no_dataset_available`
e `429` para `rate_limited`; os demais códigos são escolha deste adapter.
"""

import pytest
from fixtures import vendas_schema_com_canal

from adapters.api import create_app
from application.use_cases import ExecuteQuery, ResolveDataset
from domain.errors import QueryTimeoutError
from domain.models import Catalog, DatasourceType


def _erro(response):
    assert response.headers["content-type"].startswith("application/problem+json")
    return response.json()


def test_envelope_tem_os_campos_da_secao_2_5(client, financeiro):
    response = client.post(
        "/v1/query",
        json={"schema": "vendas", "dimensions": ["sigla_uf", "canal"]},
        headers=financeiro,
    )

    body = _erro(response)
    assert set(body) == {"type", "title", "status", "detail", "fields"}
    assert body["status"] == response.status_code


def test_unknown_schema_404(client, financeiro):
    response = client.post(
        "/v1/query", json={"schema": "produtos", "dimensions": ["nome"]}, headers=financeiro
    )

    assert response.status_code == 404
    assert _erro(response)["type"] == "unknown_schema"


def test_unknown_field_422(client, financeiro):
    response = client.post(
        "/v1/query",
        json={"schema": "vendas", "dimensions": ["sigla_uf", "canal"]},
        headers=financeiro,
    )

    assert response.status_code == 422
    body = _erro(response)
    assert body["type"] == "unknown_field"
    assert body["fields"] == ["canal"]


def test_invalid_filter_422(client, financeiro):
    """`between` não é válido para dimensão `string` (seção 2.2)."""
    response = client.post(
        "/v1/query",
        json={
            "schema": "vendas",
            "dimensions": ["sigla_uf"],
            "filters": [{"field": "sigla_uf", "operator": "between", "value": ["A", "Z"]}],
        },
        headers=financeiro,
    )

    assert response.status_code == 422
    assert _erro(response)["type"] == "invalid_filter"


def test_forbidden_measure_403(client):
    """Sem o role `financeiro`, as medidas do schema `vendas` ficam fora de alcance."""
    response = client.post(
        "/v1/query",
        json={"schema": "vendas", "dimensions": ["sigla_uf"], "measures": ["valor_total"]},
        headers={"X-Roles": "comercial"},
    )

    assert response.status_code == 403
    body = _erro(response)
    assert body["type"] == "forbidden_measure"
    assert body["fields"] == ["valor_total"]


def test_sem_header_de_roles_tambem_e_forbidden(client):
    """Ausência de roles não libera nada — negar por omissão."""
    response = client.post(
        "/v1/query",
        json={"schema": "vendas", "dimensions": ["sigla_uf"], "measures": ["valor_total"]},
    )

    assert response.status_code == 403


def test_no_dataset_available_422_com_o_exemplo_da_secao_2_5(executor, cache, job_queue):
    """O exemplo literal do documento: "Nenhum dataset do schema 'vendas' provê a
    combinação de campos: sigla_uf, cargo, canal."."""
    from fastapi.testclient import TestClient

    catalog = Catalog(schemas={"vendas": vendas_schema_com_canal()})
    execute_query = ExecuteQuery(
        catalog=catalog,
        resolve_dataset=ResolveDataset(),
        executors={DatasourceType.POSTGRES: executor, DatasourceType.ORACLE: executor},
        cache=cache,
    )
    client = TestClient(
        create_app(catalog=catalog, execute_query=execute_query, job_queue=job_queue)
    )

    response = client.post(
        "/v1/query",
        json={"schema": "vendas", "dimensions": ["sigla_uf", "cargo", "canal"]},
        headers={"X-Roles": "financeiro"},
    )

    assert response.status_code == 422
    body = _erro(response)
    assert body["type"] == "no_dataset_available"
    assert body["title"] == "Nenhum dataset cobre os campos pedidos"
    assert body["fields"] == ["sigla_uf", "cargo", "canal"]
    assert body["detail"] == (
        "Nenhum dataset do schema 'vendas' provê a combinação de campos: "
        "sigla_uf, cargo, canal."
    )


def test_query_timeout_504(client, executor, financeiro):
    executor.raises = QueryTimeoutError("A consulta excedeu 5.0s.")

    response = client.post(
        "/v1/query", json={"schema": "vendas", "dimensions": ["sigla_uf"]}, headers=financeiro
    )

    assert response.status_code == 504
    assert _erro(response)["type"] == "query_timeout"


# --- Erros de forma: não são DomainError, mas usam o mesmo envelope --------------------


def test_operador_fora_do_enum_e_malformed_request(client, financeiro):
    response = client.post(
        "/v1/query",
        json={
            "schema": "vendas",
            "dimensions": ["sigla_uf"],
            "filters": [{"field": "sigla_uf", "operator": "aproximado", "value": "SP"}],
        },
        headers=financeiro,
    )

    assert response.status_code == 422
    assert _erro(response)["type"] == "malformed_request"


def test_campo_desconhecido_no_corpo_e_recusado(client, financeiro):
    """`extra="forbid"` nos modelos Pydantic: um `limite` (em vez de `limit`) não passa
    silenciosamente como se fosse `None`."""
    response = client.post(
        "/v1/query",
        json={"schema": "vendas", "dimensions": ["sigla_uf"], "limite": 10},
        headers=financeiro,
    )

    assert response.status_code == 422
    assert _erro(response)["type"] == "malformed_request"


@pytest.mark.parametrize("payload", [{"dimensions": ["sigla_uf"]}, {"schema": "vendas"}])
def test_corpo_incompleto(client, financeiro, payload):
    response = client.post("/v1/query", json=payload, headers=financeiro)

    assert response.status_code == 422
    assert _erro(response)["type"] in {"malformed_request", "unknown_field"}
