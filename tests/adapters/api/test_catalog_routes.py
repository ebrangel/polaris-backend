"""`GET /v1/catalog` e `GET /v1/catalog/{schema}` (seção 2.1)."""


def test_lista_os_schemas_do_catalogo(client):
    response = client.get("/v1/catalog")

    assert response.status_code == 200
    nomes = [item["schema"] for item in response.json()["schemas"]]
    assert nomes == ["vendas", "eventos_navegacao", "estoque"]


def test_detalha_o_modelo_logico_do_schema(client):
    response = client.get("/v1/catalog/vendas")

    assert response.status_code == 200
    body = response.json()
    assert body["schema"] == "vendas"
    assert body["dimensions"] == [
        {"name": "sigla_uf", "type": "string", "filterable": True},
        {"name": "cargo", "type": "string", "filterable": True},
    ]
    assert body["measures"] == [
        {"name": "valor_total", "agg": "sum", "format": "currency"},
        {"name": "quantidade", "agg": "sum"},
    ]


def test_detalhe_nao_expoe_datasets_nem_roteamento(client):
    """"os datasets e seu roteamento são detalhe interno, não expostos ao cliente"
    (seção 2.1) — o cliente não pode nem descobrir que existe Oracle por trás."""
    body = client.get("/v1/catalog/vendas").text

    assert "datasets" not in body
    assert "vendas_agregado_uf" not in body
    assert "oracle" not in body.lower()
    assert "SCHEMA_DW" not in body


def test_schema_desconhecido_devolve_envelope_de_erro(client):
    response = client.get("/v1/catalog/inexistente")

    assert response.status_code == 404
    assert response.headers["content-type"].startswith("application/problem+json")
    body = response.json()
    assert body["type"] == "unknown_schema"
    assert body["status"] == 404
    assert body["fields"] == ["inexistente"]
