# Escalabilidade: fila única e pools por datasource

> **Redesenho — iteração 1**: removida a classificação leve/pesada e o estimador de
> custo. Toda consulta é enfileirada; só o worker conecta aos datasets. A API enfileira,
> aguarda o job por `INLINE_WAIT_SECONDS` e devolve o resultado inline (`200`) ou `202` +
> `poll_url`. Os trechos abaixo que ainda falam em "caminho síncrono" / "pool leve" são
> históricos.

## Princípio

Volume alto de acesso e consultas caras se resolvem tirando toda a execução analítica do
processo da API: a API só enfileira e lê resultado; o worker executa, materializa, grava
o cache e o CSV.

## Já suportado pela arquitetura base

- API stateless (catálogo em memória com invalidação via pub/sub) — permite múltiplas réplicas atrás de um load balancer sem mudança de código
- Cache Redis para consultas repetidas (escrito pelo worker)
- Contrato da API já prevê resposta síncrona e assíncrona (`status: processing` + `poll_url`) — agora toda consulta pode devolver `202`

## Ajustes necessários

### Pools de conexão separados, por datasource

Com múltiplos bancos (Postgres, Oracle via SQLAlchemy, Elasticsearch via HTTP), há **um pool por datasource**, aberto só pelo processo worker:
- Um `Engine` SQLAlchemy por `connection_ref`, com um pool (`QUERY_POOL_SIZE`) e um timeout (`QUERY_TIMEOUT_SECONDS`) — nunca compartilhado entre datasources: um datasource lento não pode esgotar conexões destinadas a outro
- Elasticsearch não usa pool de conexões relacional — o adapter dedicado usa um cliente HTTP com pool próprio (`elasticsearch-py`), com o mesmo `QUERY_TIMEOUT_SECONDS`
- O processo da API **não abre pool de datasource para executar consulta** — só o worker. A API mantém um engine de datasource apenas para o `DatasourceInspector` da publicação de catálogo (a mover para fora da API num retrabalho futuro)

### Fila de jobs (toda consulta)
- Não há estimativa de custo nem decisão síncrono/assíncrono: `ExecuteQuery` enfileira toda consulta e aguarda o job por `INLINE_WAIT_SECONDS` (`arq.jobs.Job(...).result(timeout=...)`), devolvendo o resultado inline (`200`) se concluiu ou `202` + `poll_url` se não
- Workers dedicados (processo separado da API) consomem a fila (`arq` sobre Redis), executam no pool do datasource correspondente, gravam o resultado no cache e um CSV via `ResultExporter`, e atualizam status via `query_id`
- Fila cheia (`MAX_QUEUE_DEPTH`) → backpressure: `429` ou aumento de workers

### Isolamento da carga analítica do banco transacional
- Se as tabelas também servem sistemas transacionais, usar uma réplica de leitura dedicada (ex: Active Data Guard para Oracle, réplica de leitura logical/streaming para Postgres) ou um DW separado (via ETL/CDC) só para a API analítica

### Otimizações específicas por engine

| Engine | Recursos recomendados para consultas pesadas |
|---|---|
| Oracle | Resource Manager (consumer groups), particionamento por data, bitmap indexes nas FKs de dimensão, materialized views com query rewrite, Database In-Memory (se licenciado) |
| Postgres | `work_mem` e `statement_timeout` por role, particionamento declarativo por data, índices BRIN em colunas de data, materialized views com refresh agendado, extensão `pg_stat_statements` para detectar queries lentas |
| Elasticsearch | Modelagem do índice já pré-agregada quando possível (rollup), `search.max_buckets` ajustado, réplicas de shard para distribuir leitura, agregações com `composite` para paginação eficiente |

### Rate limiting por cliente
- Limite de requisições por chave de API (`REQUEST_RATE_LIMIT`) — vale para toda submissão, já que toda consulta é assíncrona
