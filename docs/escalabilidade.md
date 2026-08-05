# Escalabilidade: consultas leves e pesadas

## Princípio

Volume alto de acesso e consultas pesadas são dois problemas de engenharia distintos e devem ser tratados com pools, filas e critérios de roteamento separados — nunca no mesmo caminho de execução.

## Já suportado pela arquitetura base

- API stateless (catálogo em memória com invalidação via pub/sub) — permite múltiplas réplicas atrás de um load balancer sem mudança de código
- Cache Redis para consultas repetidas
- Contrato da API já prevê resposta síncrona e assíncrona (`status: processing` + `poll_url`)

## Ajustes necessários

### Pools de conexão separados, por datasource

Com múltiplos bancos (Postgres, Oracle via SQLAlchemy, Elasticsearch via HTTP), a matriz de pools é (datasource × leve/pesado), não só leve/pesado:
- Um `Engine` SQLAlchemy por datasource, cada um com dois pools configurados: leve (muitas conexões, timeout curto, ex: 5s, usado só no caminho síncrono) e pesado (poucas conexões, timeout longo, usado só pelos workers assíncronos)
- Elasticsearch não usa pool de conexões relacional — o adapter dedicado usa um cliente HTTP com pool de conexões próprio (ex: `elasticsearch-py`), mas o mesmo princípio de separar timeouts curtos (síncrono) de longos (assíncrono) se aplica
- Nunca compartilhar o mesmo pool entre datasources nem entre perfis leve/pesado — uma consulta pesada segurando conexões atrasa consultas leves, e um datasource lento não pode esgotar conexões destinadas a outro

### Fila de jobs para consultas pesadas
- Critério de custo estimado (cardinalidade das dimensões pedidas, ausência de filtro seletivo, ou `EXPLAIN PLAN`/profile prévio, quando o datasource suportar) decide se a consulta é síncrona ou vai para fila — o critério é calculado por datasource, já que o custo típico varia muito entre um Postgres pequeno e um Oracle DW grande
- Workers dedicados (processo separado da API) consomem a fila (Celery+Redis ou RQ), executam no pool pesado do datasource correspondente, gravam resultado e atualizam status via `query_id`
- Fila cheia → backpressure: `429` ou aumento de workers, sem afetar consultas leves

### Isolamento da carga analítica do banco transacional
- Se as tabelas também servem sistemas transacionais, usar uma réplica de leitura dedicada (ex: Active Data Guard para Oracle, réplica de leitura logical/streaming para Postgres) ou um DW separado (via ETL/CDC) só para a API analítica

### Otimizações específicas por engine

| Engine | Recursos recomendados para consultas pesadas |
|---|---|
| Oracle | Resource Manager (consumer groups), particionamento por data, bitmap indexes nas FKs de dimensão, materialized views com query rewrite, Database In-Memory (se licenciado) |
| Postgres | `work_mem` e `statement_timeout` por role, particionamento declarativo por data, índices BRIN em colunas de data, materialized views com refresh agendado, extensão `pg_stat_statements` para detectar queries lentas |
| Elasticsearch | Modelagem do índice já pré-agregada quando possível (rollup), `search.max_buckets` ajustado, réplicas de shard para distribuir leitura, agregações com `composite` para paginação eficiente |

### Rate limiting por cliente
- Limite de requisições por chave de API
- Limite separado (mais restritivo) de consultas pesadas simultâneas em fila por cliente
