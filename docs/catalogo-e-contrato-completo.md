# Catálogo de metadados e contrato da API — consultas analíticas multi-banco

## 1. Catálogo de metadados

O catálogo é a fonte única de verdade sobre o que existe em cada banco e como pode ser consultado. Ele fica versionado em YAML no repositório e replicado em Postgres (ver `docs/pipeline-publicacao.md`) — nunca o cliente da API — determinando quais SQLs (ou, no caso do Elasticsearch, quais agregações) podem ser gerados.

### 1.0 Modelo lógico + múltiplos datasets

Um `schema` tem duas camadas:

1. **Modelo lógico** — as dimensões e medidas que o cliente da API enxerga, com nomes canônicos (`sigla_uf`, `valor_total`). É contra isso que as requisições são validadas.
2. **Datasets** — uma ou mais fontes físicas que podem atender esse modelo lógico, cada uma com seu próprio `datasource` (engine + conexão) e seu próprio mapeamento de colunas/joins. Cada dataset declara em `provides` quais dimensões e medidas do modelo lógico ele consegue atender.

Quando a requisição chega, o resolvedor de dataset percorre a lista em **ordem de declaração** e usa o primeiro cujo `provides` cubra tudo que foi pedido (dimensões, medidas, e também os campos usados em filtros/ordenação). Não há cálculo de "melhor" opção — é sempre o primeiro match. Isso faz da ordem da lista a política de otimização: coloque o dataset mais agregado/barato primeiro, o mais detalhado por último.

```yaml
version: 1
schema: vendas
description: "Vendas — dataset escolhido automaticamente por cobertura de campos"

dimensions:
  sigla_uf:
    type: string
    filterable: true
  cargo:
    type: string
    filterable: true

measures:
  valor_total:
    agg: sum
    format: currency
  quantidade:
    agg: sum

access_control:
  roles:
    financeiro: [valor_total, quantidade]

datasets:
  - name: vendas_agregado_uf
    datasource:
      type: postgres
      connection_ref: env:DW_VENDAS_PG_URL
    provides:
      dimensions: [sigla_uf]
      measures: [valor_total, quantidade]
    table:
      source: dw.vendas_agregado_uf
      mapping:
        sigla_uf: { column: uf }
        valor_total: { column: vl_total, agg: sum }
        quantidade: { column: qt_total, agg: sum }

  - name: vendas_detalhado
    datasource:
      type: oracle
      connection_ref: env:DW_VENDAS_ORACLE_URL
    provides:
      dimensions: [sigla_uf, cargo]
      measures: [valor_total, quantidade]
    fact:
      table: SCHEMA_DW.FT_VENDAS
      mapping:
        valor_total: { column: VL_TOTAL, agg: sum }
        quantidade: { column: QT_ITEM, agg: sum }
      keys:
        cliente_id: { column: CD_CLIENTE, references: dim_cliente.id }
        cargo_id: { column: CD_CARGO, references: dim_cargo.id }
    dimensions:
      dim_cliente:
        table: SCHEMA_DW.DM_CLIENTE
        primary_key: CD_CLIENTE
        mapping:
          sigla_uf: { column: SG_UF }
      dim_cargo:
        table: SCHEMA_DW.DM_CARGO
        primary_key: CD_CARGO
        mapping:
          cargo: { column: DS_CARGO }
    joins:
      - from: fato_vendas.cliente_id
        to: dim_cliente.id
      - from: fato_vendas.cargo_id
        to: dim_cargo.id
```

**Algoritmo de resolução:**

```python
def select_dataset(schema, requested_dimensions, requested_measures, filter_fields, order_fields):
    # campos usados só em filtro/ordenação também exigem cobertura,
    # mesmo que não apareçam nas colunas de saída
    required_dims = set(requested_dimensions) | set(filter_fields) | set(order_fields)

    for dataset in schema.datasets:  # ordem de declaração = prioridade
        if required_dims <= set(dataset.provides.dimensions) and \
           set(requested_measures) <= set(dataset.provides.measures):
            return dataset

    raise NoDatasetSatisfiesRequest(required_dims, requested_measures)
```

**Regras importantes:**
- Fato e dimensões **dentro de um mesmo dataset** precisam estar no mesmo datasource — não há join entre engines diferentes. Um dataset inteiro (com seus joins) sempre aponta para um único `datasource`.
- Cada dataset tem seu próprio `mapping` de nomes lógicos → colunas físicas, porque a mesma dimensão lógica (`sigla_uf`) pode ter nomes de coluna diferentes em cada fonte.
- Se nenhum dataset cobrir a requisição, a API responde com erro `no_dataset_available` (seção 2.5) — nunca tenta "montar" a resposta combinando datasets.
- `access_control` fica no nível do schema (modelo lógico), não por dataset — a permissão é sobre o dado que o cliente pode ver, não sobre onde ele está fisicamente armazenado.

### 1.1 Elasticsearch como dataset (sem joins)

Elasticsearch não é relacional — não há junção entre índices no sentido de um star schema. Um dataset com `datasource.type: elasticsearch` fica restrito a um único índice já denormalizado, sem seção `fact`/`dimensions`/`joins`, e é executado por um adapter dedicado que traduz a requisição estruturada em agregações da Query DSL — não em SQL.

```yaml
- name: eventos_navegacao_es
  datasource:
    type: elasticsearch
    connection_ref: env:ES_EVENTOS_URL
  provides:
    dimensions: [pais, dispositivo]
    measures: [duracao_media, total_eventos]
  index:
    name: eventos-navegacao
    mapping:
      pais: { field: pais, es_type: keyword }
      dispositivo: { field: dispositivo, es_type: keyword }
      duracao_media: { field: duracao_sessao, agg: avg }
      total_eventos: { field: duracao_sessao, agg: value_count }
```

O adapter não reaproveita o query builder SQLAlchemy — apenas a mesma interface de entrada/saída (requisição estruturada → colunas/linhas), para manter o contrato da API idêntico do ponto de vista do cliente. Agregações aninhadas mapeiam para *bucket aggregations* do Elasticsearch, com sintaxe própria; documentar essas limitações diretamente no código do adapter.

### 1.2 Modelo plano em banco relacional (tabela única, sem fato/dimensão)

Para datasets sem modelagem dimensional, simplifica-se: colunas são marcadas diretamente como dimensão ou medida, sem seção `fact`/`joins`.

```yaml
- name: estoque_atual_pg
  datasource:
    type: postgres
    connection_ref: env:APP_ESTOQUE_URL
  provides:
    dimensions: [filial, produto]
    measures: [quantidade_disponivel, valor_unitario]
  table:
    source: app.vw_estoque_atual
    mapping:
      filial: { column: filial }
      produto: { column: produto }
      quantidade_disponivel: { column: qtd_disponivel, agg: sum }
      valor_unitario: { column: vl_unitario, agg: avg }
```

Internamente, um dataset em modelo plano é tratado como "fato sem joins" — o query builder usa o mesmo caminho de código dos datasets em star schema, só que sem a etapa de resolução de joins.

---

## 2. Contrato da API

### 2.1 Endpoints

| Método | Rota | Função |
|---|---|---|
| GET | `/v1/catalog` | Lista os schemas (modelos lógicos) disponíveis |
| GET | `/v1/catalog/{schema}` | Detalha dimensões, medidas e datasets (e o que cada um cobre) de um modelo |
| POST | `/v1/query` | Executa (ou enfileira) uma consulta estruturada |
| GET | `/v1/query` | Mesma funcionalidade do POST, via querystring (seção 2.2a) — para curl, links de dashboard, embutir em navegador |
| GET | `/v1/query/{query_id}` | Consulta status/resultado de uma query assíncrona |

Expor `/v1/catalog` é importante: permite que quem consome a API descubra dinamicamente o que pode consultar, sem depender de documentação separada. A resposta desse endpoint mostra apenas o modelo lógico (dimensões/medidas) — os datasets e seu roteamento são detalhe interno, não expostos ao cliente.

### 2.2 `POST /v1/query` — requisição

Campos referenciam o modelo lógico diretamente — sem qualificação por tabela ou dataset, já que o cliente não sabe (nem precisa saber) qual fonte física vai atender.

```json
{
  "schema": "vendas",
  "dimensions": ["sigla_uf"],
  "measures": ["valor_total", "quantidade"],
  "filters": [
    { "field": "sigla_uf", "operator": "in", "value": ["SP", "RJ"] }
  ],
  "order_by": [
    { "field": "valor_total", "direction": "desc" }
  ],
  "limit": 100,
  "offset": 0
}
```

Essa requisição (só `sigla_uf`) seria atendida pelo dataset `vendas_agregado_uf`. Se `dimensions` incluísse também `"cargo"`, o resolvedor pularia para `vendas_detalhado`, o próximo da lista que cobre esse campo.

Operadores de filtro suportados: `eq`, `neq`, `in`, `between`, `gt`, `gte`, `lt`, `lte`, `contains`. Cada operador é validado contra o `type` do atributo no catálogo (ex: `contains` só é válido para `string`).

### 2.2a `GET /v1/query` — mesma requisição via querystring

Duas formas, escolhidas pelo cliente conforme o caso:

**Opção A — `query=<json>` (paridade total com o POST, mesmo mecanismo do Cube.js):**

```
GET /v1/query?query=%7B%22schema%22%3A%22vendas%22%2C%22dimensions%22%3A%5B%22sigla_uf%22%5D%2C%22measures%22%3A%5B%22valor_total%22%5D%7D
```

O valor de `query` é o mesmo JSON do corpo do POST (seção 2.2), apenas url-encoded. Validado pelo mesmo modelo Pydantic — zero lógica de parsing nova.

**Opção B — parâmetros planos, para uso simples (curl, links de dashboard):**

```
GET /v1/query?schema=vendas&dimensions=sigla_uf,cargo&measures=valor_total&filter[sigla_uf][in]=SP,RJ&order_by=valor_total.desc&limit=100&offset=0
```

Regras:
- `dimensions`, `measures`: lista separada por vírgula
- `filter[campo][operador]=valor`: colchetes (convenção JSON:API/Stripe); vírgula separa múltiplos valores quando o operador é `in` ou `between`; cada campo diferente é uma chave distinta, então múltiplos filtros não colidem
- `order_by`: lista separada por vírgula de pares `campo.direção` (ex: `valor_total.desc,sigla_uf.asc`)

Se `query` estiver presente, os demais parâmetros são ignorados (evita ambiguidade entre as duas formas). Ambas as opções convergem para o mesmo objeto de requisição interno antes de chegar no resolvedor de dataset — nenhuma lógica de negócio duplicada entre GET e POST.

**Limitações do GET:**
- Tamanho de URL limitado (~2000 caracteres na prática, entre navegadores/proxies) — filtros com listas grandes (ex: `in` com centenas de valores) devem usar POST
- Como `filter[campo][operador]` usa chaves dinâmicas, a rota GET recebe o `Request` cru e faz o parsing manualmente — a documentação OpenAPI desses parâmetros precisa ser escrita à mão, não é inferida automaticamente pelo FastAPI

### 2.3 Resposta — caso síncrono (consulta rápida ou em cache)

```json
{
  "query_id": "q_8f2a1c",
  "status": "completed",
  "columns": [
    { "field": "sigla_uf", "type": "string" },
    { "field": "valor_total", "type": "number", "format": "currency" },
    { "field": "quantidade", "type": "number" }
  ],
  "rows": [
    ["SP", 458320.50, 1204],
    ["RJ", 212904.10, 588]
  ],
  "meta": {
    "row_count": 2,
    "cached": true,
    "execution_ms": 12,
    "dataset_used": "vendas_agregado_uf"
  }
}
```

`meta.dataset_used` existe para depuração e observabilidade — permite ver qual fonte física atendeu cada requisição sem expor isso como algo que o cliente precisa declarar.

### 2.3a Formato de saída — JSON (padrão) ou CSV

A resposta da seção 2.3 pode ser pedida em CSV. O formato é escolhido por, nesta ordem
de precedência:

1. `?format=csv` (ou `?format=json`) na querystring — vale também no `POST`, onde o
   corpo continua sendo exatamente o objeto da seção 2.2;
2. o header `Accept: text/csv`;
3. JSON, por omissão.

O parâmetro existe além do `Accept` porque o caso de uso real do CSV é um link de
dashboard que o usuário clica para baixar, e navegador manda `Accept: text/html,…` —
negociação por header sozinha nunca entregaria CSV nesse fluxo.

**O formato não faz parte da consulta.** Ele não entra no `query_id` (seção 3) nem na
chave de cache: a mesma consulta pedida em JSON e em CSV é executada uma vez só e
compartilha a mesma entrada de cache. Por isso `format` é sempre querystring/header, e
nunca campo do corpo — um `format` dentro do corpo é rejeitado como `malformed_request`.
Pela mesma razão, `format` é a única exceção à regra "se `query` estiver presente, os
demais parâmetros são ignorados" (seção 2.2a): aquela regra é sobre a consulta, e o
formato é transporte.

O CSV segue a RFC 4180: vírgula como delimitador, registros terminados por CRLF, aspas
duplicadas quando o valor contém delimitador, aspas ou quebra de linha. A primeira
linha traz os **nomes lógicos** dos campos, os mesmos da seção 2.3 — nunca a coluna
física do dataset.

```
GET /v1/query?schema=vendas&dimensions=sigla_uf&measures=valor_total&format=csv
```

```
Content-Type: text/csv; charset=utf-8; header=present
Content-Disposition: attachment; filename="q_8f2a1c.csv"
X-Query-Id: q_8f2a1c
X-Row-Count: 2
X-Cached: true
X-Execution-Ms: 12
X-Dataset-Used: vendas_agregado_uf

sigla_uf,valor_total
SP,458320.50
RJ,212904.10
```

Como o CSV é uma grade de dados e não tem onde carregar o `meta` da seção 2.3, esses
metadados saem em headers `X-*`.

Regras de valor: `null` vira campo vazio; booleano vira `true`/`false`; data sai em ISO
8601; número decimal sai com a precisão exata do banco. O `format` declarado na coluna
(`"currency"`) é dica de apresentação para o cliente e **não** é aplicado — o CSV leva
o valor cru.

Três respostas nunca saem em CSV, qualquer que seja o formato pedido:

- o `202` de enfileiramento e o `status: processing` da seção 2.4 — `{query_id, status,
  poll_url}` não é uma tabela;
- o `status: failed`;
- qualquer erro da seção 2.5, que continua em `application/problem+json`.

O download de uma consulta pesada é, então: submeter, acompanhar `GET /v1/query/{query_id}`
em JSON e, quando `completed`, baixar com `GET /v1/query/{query_id}?format=csv`.

### 2.4 Resposta — caso assíncrono (consulta pesada)

```json
{
  "query_id": "q_9d31be",
  "status": "processing",
  "poll_url": "/v1/query/q_9d31be"
}
```

Retornado com HTTP `202 Accepted`. O cliente então consulta `GET /v1/query/{query_id}` até `status: "completed"` (ou `"failed"`), quando a resposta assume o formato da seção 2.3.

Critério sugerido para decidir síncrono vs. assíncrono: depois de resolver o dataset, estimar custo da query (cardinalidade das dimensões e ausência de filtros seletivos, específico do datasource daquele dataset) antes de executar; acima de um limiar, enfileirar.

### 2.5 Erros

Formato único de erro (estilo `application/problem+json`):

```json
{
  "type": "no_dataset_available",
  "title": "Nenhum dataset cobre os campos pedidos",
  "status": 422,
  "detail": "Nenhum dataset do schema 'vendas' provê a combinação de campos: sigla_uf, cargo, canal.",
  "fields": ["sigla_uf", "cargo", "canal"]
}
```

Tipos de erro previstos: `unknown_schema`, `unknown_field`, `invalid_filter`, `forbidden_measure` (quando o role do usuário não tem acesso à medida), `no_dataset_available` (nenhum dataset cobre a combinação pedida), `query_timeout`, `rate_limited`, `invalid_format` (valor de `format` que a API não produz, seção 2.3a).

### 2.6 Paginação

`limit`/`offset` no corpo da requisição, com `limit` máximo configurável por schema. Para exports grandes, `format=csv` (seção 2.3a) baixa direto, fora do fluxo JSON paginado. Ele ainda materializa o resultado inteiro antes de responder; um modo `csv_stream` de verdade — que transmita as linhas conforme o banco as devolve, sem passar por cache nem pela fila — exige um port de execução por streaming e fica para um marco próprio.

---

## 3. Ligação entre as peças

O `query_id` é o hash da requisição estruturada, usado como chave de cache (Redis) e como identificador de acompanhamento assíncrono. O fluxo completo de uma requisição: (1) validar campos contra o modelo lógico do schema; (2) resolver o primeiro dataset cujo `provides` cobre os campos pedidos; (3) mapear nomes lógicos para colunas físicas usando o `mapping` desse dataset; (4) gerar e executar a query no datasource correspondente (SQLAlchemy Core para bancos relacionais, adapter dedicado para Elasticsearch).
