# API de consultas analíticas multi-banco — contexto do projeto

Este arquivo é lido automaticamente pelo Claude Code como contexto persistente do projeto. Ele resume as decisões de arquitetura já tomadas; os documentos de referência completos estão em `docs/`.

## Visão geral

API REST para consultas analíticas sobre tabelas em múltiplos bancos (modelo estrela e modelo plano), com:
- Catálogo de metadados versionado em YAML (git) e replicado em **Postgres** para leitura rápida
- Cada schema expõe um **modelo lógico** (dimensões/medidas canônicas) atendido por **um ou mais datasets**, cada um com seu próprio datasource; o resolvedor escolhe em tempo de requisição o primeiro dataset (por ordem de declaração) cujo conjunto de campos cobre o que foi pedido
- Query builder que gera SQL parametrizado via **SQLAlchemy Core** para os bancos relacionais (Postgres, Oracle); **Elasticsearch** é atendido por um adapter dedicado, fora do caminho SQL
- Caminho síncrono para consultas leves e caminho assíncrono (fila + workers) para consultas pesadas

Documentos de referência (ler antes de implementar qualquer marco):
- `docs/catalogo-e-contrato-completo.md` — formato YAML do catálogo (fatos, dimensões, joins, medidas, datasource, controle de acesso) e contrato completo da API (endpoints, schemas, erros)
- `docs/pipeline-publicacao.md` — lógica de publicação incremental do catálogo (hash de conteúdo vs. banco Postgres)
- `docs/escalabilidade.md` — separação de pools leve/pesado por datasource, fila de workers, ajustes recomendados por engine

## Arquitetura de código: Clean Architecture

O projeto segue Clean Architecture. Regra de dependência: código de uma camada só pode importar de camadas mais internas — nunca o contrário. Camadas internas definem interfaces (*ports*); camadas externas as implementam.

```
src/
  domain/              # Entidades — sem dependência de framework algum
    models.py           # Schema, Dataset, Dimension, Measure, QueryRequest, QueryResult

  application/         # Use cases + ports (interfaces)
    use_cases/
      resolve_dataset.py
      execute_query.py
      publish_catalog.py
    ports/
      catalog_repository.py   # interface: get_active_version, publish_new_version
      query_executor.py       # interface: execute(dataset, query) -> QueryResult
      cache_gateway.py         # interface: get, set
      job_queue.py             # interface: enqueue, get_status

  adapters/             # Implementações concretas dos ports
    api/                 # Controllers FastAPI (routers) — POST/GET /v1/query, /v1/catalog
    repositories/        # PostgresCatalogRepository
    executors/            # SQLAlchemyQueryExecutor (Postgres/Oracle), ElasticsearchQueryExecutor
    cache/                # RedisCacheGateway
    queue/                # CeleryJobQueue

  infrastructure/        # Frameworks & drivers — composition root
    db/                    # engines SQLAlchemy por datasource
    config.py               # leitura de env vars, connection_ref
main.py                     # monta app FastAPI, injeta adapters concretos nos use cases
```

**Regra prática:** se um arquivo em `domain/` ou `application/` precisar de um `import` de `fastapi`, `sqlalchemy` ou `redis`, isso é sinal de que a camada foi violada — a dependência deveria estar invertida via port.

## Stack definida

- Linguagem/framework: Python + FastAPI
- Banco do catálogo de metadados: **Postgres** (tabela `catalog_versions`)
- Execução de consultas analíticas: **SQLAlchemy** com engines separados por datasource, todos assíncronos
  - Postgres: driver `psycopg` (modo async) via `create_async_engine`
  - Oracle: `python-oracledb` (modo thin, asyncio) via `create_async_engine`
  - Elasticsearch: **não usa SQLAlchemy Core** — adapter dedicado (`AsyncElasticsearch`) que traduz a query estruturada para a Query DSL do Elasticsearch (agregações), já que joins e subqueries não têm equivalente direto
- Cache/fila: Redis (cache de resultados + broker para fila de jobs pesados)
- Validação de schema: Pydantic

## Convenção de assincronia

Não há SQL Server no projeto: todos os engines relacionais suportados (Postgres, Oracle) e o
Elasticsearch têm driver assíncrono nativo. Por isso os *ports* de `application/ports/` e seus
adapters são inteiramente `async def` — inclusive `CatalogRepository`, `CacheGateway` e `JobQueue`,
não só `QueryExecutor`. O `domain/` continua síncrono e puro (é modelo de dados, não faz I/O); a
assincronia é característica da borda de I/O, não das entidades.

Em `application/use_cases/`, a regra é a mesma que no domínio: só é `async def` quem de fato faz
I/O (chama um port). Um use case que só orquestra objetos de domínio já carregados em memória — como
`ResolveDataset`, que percorre um `Schema` já resolvido — é síncrono; um use case que chama
`QueryExecutor`/`CacheGateway`/`JobQueue` — como `ExecuteQuery` — é `async def` e aguarda essas
chamadas com `await`. Rotas FastAPI são `async def` e chamam os use cases assíncronos com `await`.

## Convenções do projeto

- Regra de dependência da Clean Architecture: `domain/` não importa nada de `application/`, `adapters/` ou `infrastructure/`; `application/` importa apenas de `domain/` e define ports; `adapters/` implementa os ports e pode importar de `application/` e `domain/`; `infrastructure/` monta tudo (composition root em `main.py`).
- Nenhum endpoint deve montar SQL a partir de string livre — todo SQL nasce do query builder dentro do adapter `QueryExecutor`, validado contra o catálogo carregado em memória.
- O catálogo em memória de cada instância é uma cópia da tabela `catalog_versions` (Postgres); nunca ler o YAML do git em tempo de execução.
- Campos de requisição usam nomes lógicos do schema (ex: `sigla_uf`), nunca qualificados por tabela ou dataset — o cliente da API não conhece a estrutura física.
- A seleção de dataset é sempre "primeiro que cobre, por ordem de declaração no YAML" — nunca uma escolha por custo estimado ou heurística própria. A ordem no catálogo é a política de otimização.
- Joins acontecem apenas entre fato e dimensões do mesmo dataset/datasource. Não existe join federado entre datasets diferentes.
- Datasets com `datasource.type: elasticsearch` só suportam modelo plano (sem fato/dimensão) e são executados por um `QueryExecutor` dedicado, não pelo adapter SQLAlchemy.
- `POST /v1/query` e `GET /v1/query` convergem para o mesmo objeto `QueryRequest` (domain) antes de chegar no use case — nenhuma lógica de negócio duplicada entre as duas rotas.
- Toda consulta pesada deve passar pelo caminho assíncrono (fila); o critério de "pesada" está descrito em `docs/escalabilidade.md`.

## Plano de execução sugerido (marcos)

Trabalhar um marco por vez; cada um deve ser testável e revisável isoladamente antes de avançar para o próximo.

### Marco 1 — Domain: entidades
- Classes Python puras em `domain/models.py`: `Schema` (modelo lógico: dimensions/measures), `Dataset` (datasource, provides, mapping), `QueryRequest`, `QueryResult`, `Filter`, `OrderBy`
- Sem `import` de FastAPI, SQLAlchemy, Pydantic de framework ou qualquer biblioteca de infraestrutura
- Testes unitários das regras do domínio (ex: validação de que os campos em `provides` existem no schema)

### Marco 2 — Application: ports
- Interfaces abstratas em `application/ports/`: `CatalogRepository`, `QueryExecutor`, `CacheGateway`, `JobQueue`
- Ainda sem implementação concreta — só os contratos (ex: `Protocol` ou `ABC` do Python)

### Marco 3 — Application: use case de resolução de dataset
- `ResolveDataset`: dado o schema (domain) + campos pedidos, percorre `datasets` em ordem e retorna o primeiro cujo `provides` cobre tudo (dimensões, medidas, campos em filtros/ordenação); levanta erro de domínio se nenhum cobrir
- Testado com objetos de domínio puros, sem nenhum adapter — nem banco, nem HTTP

### Marco 4 — Application: use case de execução de consulta
- `ExecuteQuery`: orquestra `ResolveDataset` + chama o port `QueryExecutor` (ainda uma interface) + port `CacheGateway`
- Testado com implementações falsas (fakes/mocks) dos ports — valida a orquestração sem depender de nenhum banco real

### Marco 5 — Adapters: executores de consulta
- `SQLAlchemyQueryExecutor` (implementa `QueryExecutor`): gera SQL parametrizado via SQLAlchemy Core, um engine por datasource (Postgres, Oracle), traduzindo nomes lógicos para colunas físicas pelo `mapping` do dataset
- `ElasticsearchQueryExecutor` (implementa `QueryExecutor`): traduz a query estruturada em agregações da Query DSL
- Testes de integração cobrindo joins de star schema, modelo plano e Elasticsearch

### Marco 6 — Adapters: controllers da API
- Routers FastAPI implementando `/v1/catalog`, `/v1/catalog/{schema}`, `POST /v1/query`, `GET /v1/query` (seção 2.2a: `query=<json>` e parâmetros planos com `filter[campo][operador]`), `/v1/query/{query_id}`, conforme `docs/catalogo-e-contrato-completo.md`
- Controllers traduzem HTTP → `QueryRequest` (domain) → chamam o use case `ExecuteQuery` → traduzem `QueryResult` (domain) → resposta HTTP
- POST e GET convergem para o mesmo `QueryRequest` antes de chegar no use case
- Formato de erro padronizado (seção 2.5)

### Marco 7 — Adapters: cache e fila
- `RedisCacheGateway` (implementa `CacheGateway`) e `CeleryJobQueue` (implementa `JobQueue`)
- Critério de decisão leve/pesada (custo estimado da consulta, calculado por datasource) dentro do use case `ExecuteQuery`, delegando ao `JobQueue` quando pesada
- Pool de conexões dedicado por datasource nos executores (Marco 5)

### Marco 8 — Adapters + Infraestrutura: catálogo e pipeline de publicação
- `PostgresCatalogRepository` (implementa `CatalogRepository`)
- Use case `PublishCatalog` (application): recebe o YAML já lido, compila para JSON canônico + hash SHA-256, valida contra o modelo lógico (`domain`), chama `CatalogRepository.publish_new_version`
- Workflow de CI/CD (adapter/script): para cada schema alterado (comparação de hash, não git diff), ler o arquivo, validar semanticamente contra o datasource declarado, chamar o use case `PublishCatalog`
- Endpoint interno de publicação + invalidação de cache via pub/sub
- `infrastructure/db/`: criação dos engines SQLAlchemy por datasource; `main.py`: composition root injetando os adapters concretos nos use cases

### Marco 9 — Observabilidade
- Log de queries lentas, taxa de acerto de cache, tamanho da fila de jobs pesados
- Rate limiting por cliente

## Como pedir ao Claude Code para trabalhar em um marco

Referencie o marco específico, não o plano inteiro:

> "Implemente o Marco 1 (entidades de domínio) descrito no CLAUDE.md, usando os exemplos de docs/catalogo-e-contrato-completo.md como casos de teste."

Isso mantém cada sessão focada e revisável, em vez de tentar gerar o projeto inteiro de uma vez.
