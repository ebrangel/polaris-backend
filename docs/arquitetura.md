# Arquitetura

Mapa do código, contratos entre camadas e as decisões de projeto que explicam por que ele está
organizado assim. Para o formato do catálogo e o contrato HTTP, ver
[`catalogo-e-contrato-completo.md`](catalogo-e-contrato-completo.md).

## A regra de dependência

```
domain/          não importa nada de application/, adapters/ ou infrastructure/
application/     importa apenas domain/ — e define os ports
adapters/        implementa os ports; pode importar application/ e domain/
infrastructure/  monta tudo (composition root)
```

Camadas internas definem interfaces; camadas externas as implementam. A dependência sempre aponta
para dentro — `ExecuteQuery` não conhece Redis, conhece `CacheGateway`.

Isso não é convenção de boa vontade: [`tests/test_layer_purity.py`](../tests/test_layer_purity.py)
percorre a AST de cada arquivo em `domain/` e `application/` e falha se encontrar um `import` de
`fastapi`, `starlette`, `pydantic`, `sqlalchemy`, `redis`, `elasticsearch`, `psycopg`, `oracledb`,
`yaml`, `httpx` ou `requests` — ou de uma camada mais externa.

**Teste prático:** se um arquivo em `domain/` ou `application/` precisar de um import de framework,
a dependência deveria estar invertida via port.

## Mapa do código

### `src/domain/` — entidades

Python puro, sem framework algum. É modelo de dados e regra de negócio, não faz I/O — e por isso é
inteiramente síncrono.

| Arquivo | Conteúdo |
|---|---|
| `models.py` | `Schema`, `Dataset`, `Catalog`, `QueryRequest`, `QueryResult`, `Filter`, `OrderBy`, `TableModel`/`StarModel`/`IndexModel`, e os enums (`DataType`, `Aggregation`, `FilterOperator`, ...) |
| `errors.py` | Hierarquia de `DomainError` — um por `type` do envelope de erro |

As invariantes rodam na construção: um `Dataset` cujo `provides` cita um campo inexistente no
schema não chega a existir. `QueryRequest.query_id` é o SHA-256 da requisição canônica.

`DomainError` **não carrega código HTTP** — mapear `type` → status é responsabilidade do adapter de
API, não do domínio.

### `src/application/` — use cases e ports

| Arquivo | Papel |
|---|---|
| `ports/*.py` | Sete `Protocol`s — os contratos que `adapters/` implementa |
| `use_cases/resolve_dataset.py` | Percorre os datasets e devolve o primeiro que cobre (síncrono — só objetos em memória) |
| `use_cases/execute_query.py` | O fluxo completo de uma consulta; orquestra quase todos os ports |
| `use_cases/run_queued_query.py` | O lado worker: dataset já resolvido, executa no perfil `HEAVY` |
| `use_cases/publish_catalog.py` | Compila → compara hash → valida → publica → invalida |
| `use_cases/load_catalog.py` | Reconstrói o `Catalog` a partir do repositório |
| `use_cases/get_observability_snapshot.py` | Junta `CacheGateway.stats()` e `JobQueue.depth()` |
| `catalog_codec.py` | `dict` ↔ `Schema`, JSON canônico e hash |
| `use_cases/_executor_lookup.py`, `_slow_query_log.py` | Helpers privados compartilhados entre use cases |

### `src/adapters/` — implementações concretas

| Diretório | Implementa |
|---|---|
| `api/` | Routers FastAPI, envelope de erro (`errors.py`), parsing da querystring (`query_params.py`), modelos Pydantic e apresentadores (`schemas.py`), injeção (`dependencies.py`) |
| `executors/` | `SQLAlchemyQueryExecutor` (+ `sql_builder.py`) e `ElasticsearchQueryExecutor` (+ `elasticsearch_dsl.py`) |
| `repositories/` | `PostgresCatalogRepository` sobre `catalog_versions` |
| `cache/` | `RedisCacheGateway`, `RedisCatalogInvalidator` + assinante, `RedisRateLimiter` |
| `queue/` | `ArqJobQueue` e a task `run_heavy_query` do worker |
| `catalog/` | `yaml_loader.py` (única porta de entrada do PyYAML) e os inspetores de datasource |
| `serialization.py` | `QueryRequest`/`QueryResult` ↔ `dict`, para o broker e o cache |

### `src/infrastructure/` — composition root

| Arquivo | Papel |
|---|---|
| `config.py` | Único lugar que lê `os.environ`; resolve `connection_ref: env:NOME` → URL real |
| `db.py` | Um par de engines (leve/pesado) por `connection_ref` relacional; um cliente por `connection_ref` de Elasticsearch |
| `bootstrap.py` | Monta o `ApplicationContext`, injeta os adapters nos use cases, constrói a app e o `WorkerSettings` |

`main.py` (raiz) fica só com o `if PROCESS_ROLE == "api" / "worker"`.

## Os ports

| Port | Adapter real | Fake (`tests/fakes.py`) |
|---|---|---|
| `QueryExecutor` | `SQLAlchemyQueryExecutor`, `ElasticsearchQueryExecutor` | `StubQueryExecutor` |
| `CacheGateway` | `RedisCacheGateway` | `InMemoryCacheGateway` |
| `JobQueue` | `ArqJobQueue` | `InMemoryJobQueue` |
| `CatalogRepository` | `PostgresCatalogRepository` | `InMemoryCatalogRepository` |
| `DatasourceInspector` | `PostgresInspector`, `ElasticsearchInspector` | `StubDatasourceInspector` |
| `CatalogInvalidator` | `RedisCatalogInvalidator` | `InMemoryCatalogInvalidator` |
| `RateLimiter` | `RedisRateLimiter` | `InMemoryRateLimiter` |

Os fakes **não herdam** dos `Protocol`s — a tipagem é estrutural de propósito, para não acoplar
`tests/` a `application/`. [`tests/application/test_ports.py`](../tests/application/test_ports.py)
verifica conformidade (`isinstance`) **e** assinatura método a método, para que um fake que troque a
ordem dos parâmetros ou vire síncrono quebre ali, e não silenciosamente contra um banco real.

## O caminho de uma consulta pelas camadas

```
HTTP POST /v1/query
   │
   ▼
adapters/api/routers/query.py          traduz HTTP → QueryRequestModel (Pydantic)
   │                                   resolve X-Roles e X-Api-Key via dependencies.py
   ▼
   .to_domain() ──────────────────────► domain/models.py: QueryRequest (invariantes validam aqui)
   │
   ▼
application/use_cases/execute_query.py orquestra: rate limit → valida → autoriza → cache →
   │                                   resolve dataset → estima custo → executa → cacheia
   ├──► ResolveDataset          (domínio puro, síncrono)
   ├──► CacheGateway            ──► RedisCacheGateway
   ├──► QueryExecutor           ──► SQLAlchemyQueryExecutor ──► sql_builder.py ──► Postgres/Oracle
   │                             ou  ElasticsearchQueryExecutor ──► elasticsearch_dsl.py ──► ES
   └──► JobQueue                ──► ArqJobQueue (se pesada)
   │
   ▼
domain/models.py: QueryResult
   │
   ▼
adapters/api/schemas.py: present_result() ──► JSON das seções 2.3/2.4
```

O caminho assíncrono repete a metade de baixo dentro do worker:
`adapters/queue/tasks.py` → `RunQueuedQuery` → `QueryExecutor` (perfil `HEAVY`).

## Decisões de projeto

### Roteamento por `connection_ref`, não por `DatasourceType`

Os executores e inspetores são chaveados pelo `connection_ref` do dataset (`env:DW_VENDAS_PG_URL`),
**não** pelo tipo de engine.

O catálogo de exemplo tem dois Postgres distintos — `env:DW_VENDAS_PG_URL` e `env:APP_ESTOQUE_URL`.
Chavear por `DatasourceType.POSTGRES` faria os dois compartilharem engine e pool, contrariando o
"nunca compartilhar o mesmo pool entre datasources" de
[`escalabilidade.md`](escalabilidade.md): um banco lento esgotaria conexões destinadas ao outro.

### Assincronia só onde há I/O

Todos os ports e adapters são `async def` — inclusive `CatalogRepository`, `CacheGateway` e
`JobQueue`, não só `QueryExecutor`. Não há SQL Server no projeto: todos os engines suportados têm
driver assíncrono nativo, então não sobra nenhuma ponte `asyncio.to_thread`.

Nos use cases a regra é a mesma do domínio: **só é `async def` quem de fato faz I/O**.
`ResolveDataset`, que percorre um `Schema` já em memória, é síncrono; `ExecuteQuery`, que chama
ports, é `async`.

### O codec fica em `application/`, não em `adapters/`

`catalog_codec.py` traduz `dict` ↔ `Schema`. Um `dict` é estrutura Python pura, não framework — e o
codec é o contrato canônico do catálogo, usado tanto pelo use case `PublishCatalog` quanto pelo
`PostgresCatalogRepository` ao reconstruir uma versão lida do banco. Deixá-lo no adapter obrigaria
`application/` a importar de `adapters/`.

O PyYAML entra só em `adapters/catalog/yaml_loader.py`, que é onde a dependência de fato existe.

### O hash é do conteúdo compilado

`PublishCatalog` compila o YAML para JSON canônico (chaves ordenadas, separadores compactos) antes
de calcular o SHA-256. Reordenar chaves, reindentar o arquivo ou acrescentar uma chave não modelada
**não** dispara republicação — e o que fica gravado é exatamente o que o repositório vai ler de
volta.

Frozensets viram listas ordenadas na serialização (`sorted(dataset.provides.dimensions)`): sem isso,
a ordem de iteração de um `set` de strings variaria entre processos e o hash seria instável.

### `bootstrap.py` separado de `main.py`

`main.py` constrói a app como **efeito colateral do import** — é o que permite
`uvicorn main:app`. Logo, nada mais pode importar `main.py` sem pagar esse custo.

`scripts/publish_catalog.py` importa só de `infrastructure/bootstrap.py`, sem efeito colateral.

### A montagem com I/O acontece no `lifespan`, não no import

`create_application()` é síncrono e só monta a app vazia: o uvicorn importa `main:app` de dentro do
event loop dele (`Server.serve` → `config.load()`), então um `asyncio.run()` no import falha com
*"asyncio.run() cannot be called from a running event loop"*.

Quem lê o catálogo do Postgres, abre os pools do Redis e injeta os use cases em `app.state` é o
`lifespan` — que roda no loop do servidor, antes da primeira requisição, e desmonta tudo na saída.
As dependências de `dependencies.py` já liam `app.state` a cada requisição, então nada mais mudou.
O worker tem a mesma divisão: `create_worker_settings()` é síncrono e o `on_startup` do arq monta o
`RunQueuedQuery` já no loop do worker.

### O script de publicação tem fiação própria

`build_context()` monta engines a partir do catálogo **já ativo no banco** — correto para a API e o
worker, que só executam contra o que está publicado. Errado para o script de publicação: na primeira
publicação de um schema novo (ou de um dataset com `connection_ref` novo), essa versão ativa ainda
não existe, e os inspetores necessários nunca seriam construídos.

Por isso `scripts/publish_catalog.py` deriva engines e inspetores **dos arquivos YAML que está
publicando**, não do estado anterior.

### SQLAlchemy Core, não ORM

Não há entidades persistentes para mapear — o que existe é geração dinâmica de `SELECT` a partir de
metadados. O Core dá exatamente isso, com SQL parametrizado e dialeto por engine.

**Nenhum endpoint monta SQL a partir de string livre.** Todo SQL nasce do query builder dentro do
adapter, validado contra o catálogo em memória.

### Elasticsearch fora do caminho SQL

Joins e subqueries não têm equivalente direto na Query DSL, então o `ElasticsearchQueryExecutor` não
reaproveita o query builder — só a mesma interface de entrada/saída (requisição estruturada →
colunas/linhas). Datasets Elasticsearch são restritos ao modelo plano (`index:`), sem
`fact`/`dimensions`/`joins`.

### `arq` no lugar de Celery

`docs/escalabilidade.md` admite "Celery+Redis ou RQ" como equivalentes. O `arq` é async-nativo sobre
Redis, o que evita pontes `asyncio.to_thread` num port inteiramente `async def`.

`ArqJobQueue.enqueue` usa o `query_id` como `_job_id`: duas requisições idênticas reaproveitam o
mesmo job em vez de duplicá-lo na fila.

### Erros de domínio não sabem HTTP

`DomainError` carrega `type`, `title`, `detail` e `fields` — mas não `status`. O mapa `type` → código
HTTP vive em [`adapters/api/errors.py`](../src/adapters/api/errors.py). Um handler único cobre a
hierarquia inteira, porque todos os erros herdam de `DomainError`.

## Como estender

### Um novo tipo de datasource

1. Acrescente o valor em `DatasourceType` (`domain/models.py`).
2. Se for relacional com dialeto SQLAlchemy, `SQLAlchemyQueryExecutor` já atende — basta ensinar
   `infrastructure/db.py` a construir o engine. Se não for, escreva um `QueryExecutor` novo em
   `adapters/executors/`.
3. Ensine `catalog_codec.py` a ler/escrever o formato físico, se ele introduzir uma seção nova.
4. Opcionalmente, escreva um `DatasourceInspector` para validação semântica na publicação. Sem ele,
   os datasets são publicados e reportados como "sem inspeção" — nunca silenciosamente.
5. Monte o executor em `bootstrap.build_context()`, chaveado por `connection_ref`.

### Um novo port

1. Defina o `Protocol` em `application/ports/` e exporte-o em `ports/__init__.py`.
2. Implemente em `adapters/`.
3. Adicione um fake em `tests/fakes.py` e registre o par em `PORT_METHODS`
   (`tests/application/test_ports.py`) — isso já cobre conformidade e assinatura.
4. Injete no use case pelo construtor, com default `None` se for opcional.

### Um novo endpoint

1. Router em `adapters/api/routers/`, usando os `Dep` de `dependencies.py`.
2. Inclua em `create_app()` (`adapters/api/app.py`). Se depender de uma peça opcional, monte o router
   condicionalmente, como fazem `admin` e `observability`.
3. Traduza HTTP → domínio → HTTP no router. **Nenhuma regra de negócio no adapter.**

### Autenticação real

Hoje `X-Roles`, `X-Api-Key` e `X-Internal-Token` são stand-ins lidos em
[`adapters/api/dependencies.py`](../src/adapters/api/dependencies.py). Trocar a origem dos roles por
um token verificado é mudança local a `get_roles()` / `get_client_id()` — o caminho HTTP → use case
já existe e é explícito.
