# Polaris — API de consultas analíticas multi-banco

API REST que expõe **um modelo lógico único** sobre tabelas espalhadas em bancos diferentes
(Postgres, Oracle, Elasticsearch). O cliente pede dimensões e medidas por nome canônico; o servidor
decide qual fonte física atende, gera a consulta e executa — sem que quem consome saiba onde o dado
mora.

```jsonc
// O cliente pede isto...
POST /v1/query
{ "schema": "vendas", "dimensions": ["sigla_uf"], "measures": ["valor_total"] }
// → atendido por `vendas_agregado_uf` (tabela agregada em Postgres)

// ...e isto, que só muda uma dimensão:
{ "schema": "vendas", "dimensions": ["sigla_uf", "cargo"], "measures": ["valor_total"] }
// → atendido por `vendas_detalhado` (star schema em Oracle, com dois JOINs)
```

Nenhuma das duas requisições menciona tabela, coluna, banco ou dataset. A escolha da fonte é do
servidor, guiada pelo catálogo.

---

## Índice

- [Conceitos centrais](#conceitos-centrais)
- [Ciclo de vida de uma requisição](#ciclo-de-vida-de-uma-requisição)
- [Começando](#começando)
- [Endpoints](#endpoints)
- [O catálogo](#o-catálogo)
- [Arquitetura](#arquitetura)
- [Testes](#testes)
- [Estrutura do projeto](#estrutura-do-projeto)
- [Documentação](#documentação)

---

## Conceitos centrais

### Modelo lógico × datasets

Um **schema** tem duas camadas:

| Camada | O que é | Quem enxerga |
|---|---|---|
| **Modelo lógico** | Dimensões e medidas com nomes canônicos (`sigla_uf`, `valor_total`) | O cliente da API |
| **Datasets** | Uma ou mais fontes físicas que atendem esse modelo, cada uma com seu datasource e mapeamento de colunas | Só o servidor |

Cada dataset declara em `provides` quais dimensões/medidas do modelo lógico ele cobre.

### Resolução de dataset: "primeiro que cobre"

O resolvedor percorre os datasets **na ordem de declaração do YAML** e usa o primeiro cujo `provides`
cubra tudo que foi pedido — incluindo campos que aparecem só em filtros ou ordenação. Não há cálculo
de custo nem heurística de "melhor opção".

> **A ordem no catálogo é a política de otimização.** Coloque o dataset mais agregado/barato
> primeiro e o mais detalhado por último.

Se nenhum dataset cobre a combinação, a resposta é `no_dataset_available` (HTTP 422) — a API nunca
tenta montar o resultado combinando datasets.

### Três formatos físicos

| Formato | Seção do YAML | Engine |
|---|---|---|
| **Modelo plano** — tabela/visão única | `table:` | Postgres, Oracle |
| **Star schema** — fato + dimensões + joins | `fact:` + `dimensions:` + `joins:` | Postgres, Oracle |
| **Índice denormalizado** — sem joins | `index:` | Elasticsearch |

Joins acontecem **apenas dentro de um mesmo dataset e datasource**. Não existe join federado entre
bancos diferentes.

### Leve × pesada

Depois de resolver o dataset, a API estima o custo da consulta antes de executar:

- **Postgres** — `EXPLAIN (FORMAT JSON)` real contra o banco
- **Oracle / Elasticsearch** — heurística por contagem de campos (`dimensões × 10 − filtros × 5`)

Acima do limiar (`COST_THRESHOLD`), a consulta vai para a fila de workers e a API responde `202` com
`poll_url`. Abaixo, executa de forma síncrona no pool leve. Os dois caminhos usam **pools de conexão
separados por datasource** — uma consulta pesada nunca segura conexões do caminho síncrono.

---

## Ciclo de vida de uma requisição

```
POST /v1/query
      │
      ├─ 0. rate limit por cliente ──────────────► 429 rate_limited
      ├─ 1. valida campos contra o modelo lógico ► 422 unknown_field / invalid_filter
      ├─ 2. autoriza medidas pelo role ──────────► 403 forbidden_measure
      ├─ 3. aplica o teto de `limit` do schema
      ├─ 4. consulta o cache pelo `query_id` ────► 200 (meta.cached = true)
      ├─ 5. resolve o dataset ───────────────────► 422 no_dataset_available
      ├─ 6. estima o custo
      │      ├─ pesada ─► fila cheia? ───────────► 429 rate_limited
      │      │            limite do cliente? ────► 429 rate_limited
      │      │            senão ─────────────────► 202 processing + poll_url
      │      └─ leve ─► executa (loga se lenta) ─► 200 completed
      └─ 7. grava no cache
```

O `query_id` (`q_8f2a1c`) é o **hash SHA-256 da requisição canônica** — serve ao mesmo tempo como
chave de cache e como identificador de acompanhamento assíncrono. Duas requisições idênticas
produzem o mesmo `query_id`, então compartilham cache e nunca duplicam um job na fila.

---

## Começando

### Pré-requisitos

- **Python 3.12+**
- **[uv](https://docs.astral.sh/uv/)** para dependências
- **Docker** — para Postgres/Redis locais e para os testes de integração

### 1. Instalar

```bash
git clone <url-do-repositorio>
cd polaris-backend
uv sync
```

### 2. Subir a infraestrutura

O catálogo precisa de um Postgres próprio (separado de qualquer datasource analítico) e de um Redis
(cache + fila + pub/sub):

```bash
docker run -d --name polaris-pg    -e POSTGRES_PASSWORD=polaris -p 5432:5432 postgres:16-alpine
docker run -d --name polaris-redis -p 6379:6379 redis:7-alpine
```

### 3. Configurar o ambiente

```bash
export CATALOG_DB_URL="postgresql+psycopg://postgres:polaris@localhost:5432/postgres"
export REDIS_URL="redis://localhost:6379/0"
export INTERNAL_TOKEN="um-token-qualquer-para-dev"

# Um por `connection_ref` declarado no catálogo (formato `env:NOME`):
export APP_ESTOQUE_URL="postgresql+psycopg://postgres:polaris@localhost:5432/postgres"
export DW_VENDAS_PG_URL="postgresql+psycopg://postgres:polaris@localhost:5432/postgres"
export DW_VENDAS_ORACLE_URL="oracle+oracledb://user:pass@localhost:1521/?service_name=XEPDB1"
export ES_EVENTOS_URL="http://localhost:9200"
```

> **Atenção:** o boot resolve **todos** os `connection_ref` dos schemas ativos no catálogo. Como
> `vendas.yaml` declara um dataset Oracle, `DW_VENDAS_ORACLE_URL` precisa estar definida mesmo sem um
> Oracle rodando — a URL só é lida para *construir* o engine; a conexão só abre na primeira consulta
> àquele dataset. O mesmo vale para `ES_EVENTOS_URL`. Os valores fictícios acima bastam para o
> passeio: só as consultas que caírem nesses datasets é que vão falhar.

### 4. Criar a tabela do catálogo

O projeto não tem ferramenta de migração — a tabela `catalog_versions` precisa existir antes da
primeira publicação. O DDL está declarado como metadata do SQLAlchemy em
[`adapters/repositories/postgres_catalog_repository.py`](src/adapters/repositories/postgres_catalog_repository.py),
e `create_tables()` aplica ele (tabela + índice parcial):

```bash
uv run python -c "
import asyncio, sys; sys.path.insert(0, 'src')
from sqlalchemy.ext.asyncio import create_async_engine
from adapters.repositories.postgres_catalog_repository import create_tables
import os

async def main():
    engine = create_async_engine(os.environ['CATALOG_DB_URL'])
    await create_tables(engine)
    await engine.dispose()

asyncio.run(main())
"
```

O DDL equivalente em SQL puro está em [`docs/pipeline-publicacao.md`](docs/pipeline-publicacao.md),
caso prefira aplicá-lo pelo `psql` ou por uma migração própria.

### 5. Criar as tabelas de exemplo

A publicação **valida o catálogo contra o datasource real** — se uma coluna declarada no `mapping`
não existir, o schema é recusado. Para o passeio, crie as tabelas que os exemplos referenciam:

```bash
docker exec -i polaris-pg psql -U postgres -q <<'SQL'
CREATE SCHEMA IF NOT EXISTS dw;
CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE dw.vendas_agregado_uf (uf text, vl_total numeric, qt_total numeric);
INSERT INTO dw.vendas_agregado_uf VALUES ('SP', 458320.50, 1204), ('RJ', 212904.10, 588);

CREATE TABLE app.vw_estoque_atual (filial text, produto text, qtd_disponivel numeric, vl_unitario numeric);
INSERT INTO app.vw_estoque_atual VALUES
  ('Matriz',    'Cabo HDMI', 120, 29.90),
  ('Filial SP', 'Cabo HDMI',  45, 31.50),
  ('Matriz',    'Mouse',     300, 89.00);
SQL
```

Sem isso a publicação falha com `invalid_catalog`, listando exatamente as colunas que faltam — que é
o comportamento desejado, só não o que se quer no primeiro passeio.

### 6. Publicar o catálogo

O `catalog/schemas/` já vem com os três exemplos do contrato (`vendas`, `eventos_navegacao`,
`estoque`). O script compila cada YAML, compara o hash com a versão ativa, valida contra o
datasource real e publica o que mudou:

```bash
uv run python scripts/publish_catalog.py
```

```
publicado  estoque @ a2e2e7818f9b
publicado  eventos_navegacao @ 1a9abe3d776b (sem inspeção de datasource: eventos_navegacao_es)
publicado  vendas @ c41b5575d796 (sem inspeção de datasource: vendas_detalhado)
```

Datasets cujo datasource não tem inspetor (Oracle) ou cujo cliente não está configurado aparecem
como *"sem inspeção"* — são publicados, mas sem validação semântica. Se uma coluna declarada no
`mapping` não existir no banco, a publicação **falha** e o schema não entra no ar.

### 7. Rodar

```bash
# API
uv run uvicorn main:app --reload

# Worker de consultas pesadas (outro terminal)
PROCESS_ROLE=worker uv run arq main.WorkerSettings
```

Documentação interativa (Swagger) em <http://localhost:8000/docs>.

### 8. Primeira consulta

```bash
curl -s localhost:8000/v1/catalog | jq

curl -s -X POST localhost:8000/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"schema":"estoque","dimensions":["filial"],"measures":["quantidade_disponivel"]}' | jq
```

---

## Endpoints

### Públicos

| Método | Rota | Função |
|---|---|---|
| `GET` | `/v1/catalog` | Lista os schemas disponíveis |
| `GET` | `/v1/catalog/{schema}` | Dimensões e medidas do modelo lógico |
| `POST` | `/v1/query` | Executa ou enfileira uma consulta |
| `GET` | `/v1/query` | Idem, via querystring |
| `GET` | `/v1/query/{query_id}` | Status/resultado de uma consulta assíncrona |

`GET /v1/catalog/{schema}` devolve **apenas o modelo lógico** — datasets e roteamento são detalhe
interno e não são expostos.

### Internos (exigem `X-Internal-Token`)

| Método | Rota | Função |
|---|---|---|
| `POST` | `/internal/catalog/publish` | Publica um schema (usado pelo CI) |
| `POST` | `/internal/catalog/reload` | Recarrega o catálogo em memória desta instância |
| `GET` | `/internal/observability` | Taxa de acerto do cache e profundidade da fila |

Essas rotas só são montadas quando o composition root injeta as dependências correspondentes — em
testes que chamam `create_app()` sem elas, respondem `404`, não `401`.

### Headers

| Header | Uso | Situação |
|---|---|---|
| `X-Roles` | Lista separada por vírgula; libera medidas com `access_control` | **Stand-in** — não há autenticação real ainda |
| `X-Api-Key` | Identifica o cliente para rate limiting; sem ele, cai no IP | **Stand-in** |
| `X-Internal-Token` | Autoriza as rotas `/internal/*` | **Stand-in** |

### `GET /v1/query` — duas formas

```bash
# Opção A — o mesmo JSON do POST, url-encoded (paridade total)
GET /v1/query?query=%7B%22schema%22%3A%22vendas%22%2C%22dimensions%22%3A%5B%22sigla_uf%22%5D%7D

# Opção B — parâmetros planos, para curl e links de dashboard
GET /v1/query?schema=vendas&dimensions=sigla_uf,cargo&measures=valor_total\
&filter[sigla_uf][in]=SP,RJ&order_by=valor_total.desc&limit=100
```

Se `query` estiver presente, os demais parâmetros são ignorados. As duas formas convergem para o
mesmo objeto de domínio antes de chegar ao use case — nenhuma regra de negócio é duplicada.

### Erros

Formato único, estilo `application/problem+json`:

```json
{
  "type": "no_dataset_available",
  "title": "Nenhum dataset cobre os campos pedidos",
  "status": 422,
  "detail": "Nenhum dataset do schema 'vendas' provê a combinação de campos: sigla_uf, cargo, canal.",
  "fields": ["sigla_uf", "cargo", "canal"]
}
```

| `type` | HTTP | Quando |
|---|---|---|
| `unknown_schema` | 404 | Schema não existe no catálogo |
| `unknown_field` | 422 | Campo não existe no modelo lógico |
| `invalid_filter` | 422 | Operador incompatível com o tipo do campo |
| `forbidden_measure` | 403 | O role não alcança a medida |
| `no_dataset_available` | 422 | Nenhum dataset cobre a combinação |
| `query_timeout` | 504 | O datasource estourou o prazo |
| `rate_limited` | 429 | Limite por cliente ou fila cheia |
| `invalid_catalog` | 422 | Catálogo malformado (publicação) |
| `malformed_request` | 422 | Erro de forma (JSON/enum/tipo) |
| `unknown_query` | 404 | `query_id` inexistente |

---

## O catálogo

Versionado em YAML no git (`catalog/schemas/*.yaml`), replicado em Postgres para leitura rápida.
**Git nunca é lido em tempo de execução** — cada instância carrega o catálogo da tabela
`catalog_versions` no boot e o mantém em memória.

```yaml
version: 1
schema: estoque

dimensions:
  filial:  { type: string, filterable: true }
  produto: { type: string, filterable: true }

measures:
  quantidade_disponivel: { agg: sum }
  valor_unitario:        { agg: avg }

datasets:
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
        filial:                { column: filial }
        produto:               { column: produto }
        quantidade_disponivel: { column: qtd_disponivel, agg: sum }
        valor_unitario:        { column: vl_unitario,    agg: avg }
```

### Publicação incremental

O hash é do **conteúdo compilado**, não do YAML cru — reordenar chaves ou reindentar o arquivo não
dispara republicação. A comparação é por hash, nunca por `git diff` (que quebra com
squash/rebase/force-push).

```
YAML → compila (valida contra o domínio) → SHA-256
                                             │
                    hash == versão ativa? ───┴─► sim: pula, nada a fazer
                                                 não: valida contra o datasource real
                                                      → grava nova linha (a anterior é desativada
                                                        na mesma transação)
                                                      → publica em `catalog:invalidate` (Redis)
                                                        → as réplicas recarregam na hora
```

Cada publicação é uma **linha nova** — nunca um `UPDATE` no conteúdo. Isso dá histórico completo e
rollback trivial. Um índice parcial (`ux_catalog_active`) garante no próprio banco que só existe
uma versão ativa por schema.

Formato completo em [`docs/catalogo-e-contrato-completo.md`](docs/catalogo-e-contrato-completo.md);
pipeline em [`docs/pipeline-publicacao.md`](docs/pipeline-publicacao.md).

---

## Arquitetura

Clean Architecture, com a regra de dependência verificada automaticamente por
[`tests/test_layer_purity.py`](tests/test_layer_purity.py) — um `import` de `fastapi`, `sqlalchemy`
ou `redis` dentro de `domain/` ou `application/` quebra a suíte.

```
        ┌─────────────────────────────────────────────┐
        │  infrastructure/   composition root          │  ← lê env, abre engines, injeta tudo
        │  ┌───────────────────────────────────────┐  │
        │  │  adapters/     FastAPI, SQLAlchemy,   │  │  ← implementa os ports
        │  │                Redis, arq, PyYAML      │  │
        │  │  ┌─────────────────────────────────┐  │  │
        │  │  │  application/  use cases + ports │  │  │  ← orquestra; define interfaces
        │  │  │  ┌───────────────────────────┐  │  │  │
        │  │  │  │  domain/   entidades puras │  │  │  │  ← zero dependência de framework
        │  │  │  └───────────────────────────┘  │  │  │
        │  │  └─────────────────────────────────┘  │  │
        │  └───────────────────────────────────────┘  │
        └─────────────────────────────────────────────┘
             As setas de dependência apontam para dentro.
```

Sete **ports** (`typing.Protocol`) isolam o núcleo da infraestrutura: `QueryExecutor`,
`CacheGateway`, `JobQueue`, `CatalogRepository`, `DatasourceInspector`, `CatalogInvalidator` e
`RateLimiter`. Cada um tem uma implementação real em `adapters/` e um fake in-memory em
`tests/fakes.py`.

Detalhamento — mapa de arquivos, decisões de projeto e como estender — em
[`docs/arquitetura.md`](docs/arquitetura.md).

### Stack

| Camada | Tecnologia |
|---|---|
| API | FastAPI + Pydantic |
| SQL | SQLAlchemy **Core** (não ORM), engines assíncronos por datasource |
| Postgres | `psycopg` (async) |
| Oracle | `python-oracledb` (thin, asyncio) |
| Elasticsearch | `AsyncElasticsearch` — adapter dedicado, fora do caminho SQL |
| Cache / pub-sub / rate limit | Redis |
| Fila | `arq` (async-nativo sobre Redis) |
| Catálogo | YAML no git → Postgres |

---

## Testes

**380 testes**, divididos entre rápidos e de integração:

```bash
uv run pytest -q -m "not integration"   # 333 testes, ~1,5 s, sem Docker
uv run pytest -q                        # 380 testes, ~40 s, com containers reais
```

Os testes de integração sobem **Postgres, Elasticsearch e Redis de verdade** via
[testcontainers](https://testcontainers.com/) — sem mocks de banco. Cada módulo verifica se o Docker
está disponível e se autopula quando não está, então a suíte rápida roda em qualquer lugar.

| O que é testado | Onde |
|---|---|
| Invariantes do domínio | `tests/domain/` |
| Orquestração dos use cases (com fakes) | `tests/application/` |
| SQL gerado, Query DSL, execução real | `tests/adapters/executors/` |
| Contrato HTTP, envelope de erro, rate limiting | `tests/adapters/api/` |
| Cache, fila, pub/sub, rate limiter | `tests/adapters/cache/`, `tests/adapters/queue/` |
| Repositório do catálogo e inspetores | `tests/adapters/repositories/`, `tests/adapters/catalog/` |
| Regra de dependência entre camadas | `tests/test_layer_purity.py` |

Os exemplos do contrato viram casos de teste literais: `tests/fixtures.py` reproduz os três YAMLs da
documentação como objetos de domínio, e `tests/application/test_catalog_codec.py` prova que carregar
`catalog/schemas/vendas.yaml` produz exatamente o mesmo `Schema` que o fixture.

---

## Estrutura do projeto

```
├── catalog/schemas/            # Catálogo versionado em YAML
├── docs/                       # Documentação de referência
├── scripts/publish_catalog.py  # Publicação incremental (chamado pelo CI)
├── main.py                     # Entry point (API ou worker, por PROCESS_ROLE)
├── src/
│   ├── domain/                 # Entidades puras — models.py, errors.py
│   ├── application/
│   │   ├── ports/              # Interfaces (Protocol)
│   │   ├── use_cases/          # ExecuteQuery, ResolveDataset, PublishCatalog, ...
│   │   └── catalog_codec.py    # dict ↔ Schema, JSON canônico, hash
│   ├── adapters/
│   │   ├── api/                # Routers FastAPI, envelope de erro, parsing de querystring
│   │   ├── executors/          # SQLAlchemy (Postgres/Oracle) e Elasticsearch
│   │   ├── repositories/       # PostgresCatalogRepository
│   │   ├── cache/              # Cache, pub/sub e rate limiter em Redis
│   │   ├── queue/              # ArqJobQueue + task do worker
│   │   └── catalog/            # Leitor de YAML e inspetores de datasource
│   └── infrastructure/         # config.py, db.py, bootstrap.py
└── tests/                      # 380 testes (fakes + testcontainers)
```

---

## Documentação

| Documento | Conteúdo |
|---|---|
| [`docs/arquitetura.md`](docs/arquitetura.md) | Mapa do código, ports, decisões de projeto, como estender |
| [`docs/operacao.md`](docs/operacao.md) | Variáveis de ambiente, processos, deploy, observabilidade |
| [`docs/catalogo-e-contrato-completo.md`](docs/catalogo-e-contrato-completo.md) | Formato do catálogo e contrato da API, completos |
| [`docs/pipeline-publicacao.md`](docs/pipeline-publicacao.md) | Publicação incremental do catálogo |
| [`docs/escalabilidade.md`](docs/escalabilidade.md) | Pools, fila, ajustes por engine, rate limiting |
| [`CLAUDE.md`](CLAUDE.md) | Contexto persistente para o Claude Code, com o plano de marcos |

---

## Status

Os nove marcos do plano estão implementados: domínio, ports, resolução de dataset, execução de
consulta, executores, controllers, cache e fila, catálogo e pipeline de publicação, e
observabilidade.

**Pontos deliberadamente fora de escopo** (e por quê):

- **Autenticação real** — `X-Roles`, `X-Api-Key` e `X-Internal-Token` são stand-ins explícitos.
  Trocar a origem dos roles por um token verificado é mudança local a `adapters/api/dependencies.py`.
- **Inspetor de Oracle** na validação de publicação — exigiria um container Oracle nos testes.
  Datasets Oracle são publicados e reportados como "sem inspeção", nunca silenciosamente.
- **Manifesto `dependencies.yaml`** para dimensões compartilhadas entre schemas — enquanto o
  catálogo for pequeno, schemas autocontidos são mais simples (recomendação do próprio doc de
  pipeline).
- **`format: "csv_stream"`** para exports grandes — previsto na seção 2.6 do contrato, não
  implementado.
