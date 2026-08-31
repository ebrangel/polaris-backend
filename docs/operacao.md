# Operação

Referência de configuração, processos e observabilidade. Para o desenho por trás das decisões de
escala, ver [`escalabilidade.md`](escalabilidade.md).

## Variáveis de ambiente

Lidas uma única vez, no boot, por [`infrastructure/config.py`](../src/infrastructure/config.py). É o
único lugar do sistema que toca `os.environ` — o resto recebe configuração já resolvida.

### Obrigatórias

| Variável | Descrição |
|---|---|
| `CATALOG_DB_URL` | Postgres do catálogo, com driver async (`postgresql+psycopg://...`). Infraestrutura própria — separada de qualquer datasource analítico |
| `REDIS_URL` | Redis usado por cache, fila, pub/sub e rate limiting |
| `INTERNAL_TOKEN` | Valor esperado no header `X-Internal-Token` das rotas `/internal/*` |

A ausência de qualquer uma derruba o boot com `ConfigError` — não há default silencioso.

### Conexões dos datasources

Além das acima, **cada `connection_ref` declarado nos schemas ativos** precisa da env var
correspondente. O formato `env:NOME` é o único suportado:

```yaml
datasource:
  connection_ref: env:DW_VENDAS_PG_URL   # → lê a env var DW_VENDAS_PG_URL
```

Com o catálogo de exemplo completo, isso significa `DW_VENDAS_PG_URL`, `DW_VENDAS_ORACLE_URL`,
`ES_EVENTOS_URL` e `APP_ESTOQUE_URL`.

> O boot resolve todos os `connection_ref` dos schemas ativos para **construir** os engines. A
> conexão só é aberta na primeira consulta àquele dataset — mas a variável precisa existir desde o
> início.

### Opcionais

| Variável | Default | Efeito |
|---|---|---|
| `PROCESS_ROLE` | `api` | `api` monta a app FastAPI; `worker` monta o `WorkerSettings` do arq |
| `GIT_SHA` | `unknown` | Gravado em `catalog_versions.git_sha` na publicação |
| `LOG_LEVEL` | `INFO` | Nível do `logging.basicConfig` |
| `LIGHT_TIMEOUT_SECONDS` | `5.0` | Timeout do caminho síncrono |
| `HEAVY_TIMEOUT_SECONDS` | `300.0` | Timeout do caminho assíncrono (workers) |
| `LIGHT_POOL_SIZE` | `20` | Conexões do pool leve, **por datasource** |
| `HEAVY_POOL_SIZE` | `3` | Conexões do pool pesado, **por datasource** |
| `COST_THRESHOLD` | `10000.0` | Acima disso a consulta vai para a fila |
| `CACHE_TTL_SECONDS` | `3600` | TTL padrão das entradas de cache |
| `SLOW_QUERY_THRESHOLD_MS` | `2000` | Consultas acima disso geram log `WARNING` |
| `REQUEST_RATE_LIMIT` | `100` | Requisições por cliente, por janela |
| `REQUEST_RATE_LIMIT_WINDOW_SECONDS` | `60` | Tamanho da janela do limite geral |
| `HEAVY_QUERY_RATE_LIMIT` | `5` | Consultas pesadas por cliente, por janela |
| `HEAVY_QUERY_RATE_LIMIT_WINDOW_SECONDS` | `60` | Tamanho da janela do limite pesado |
| `MAX_HEAVY_QUEUE_DEPTH` | `100` | Profundidade da fila que dispara backpressure (`429`) |
| `EXPORT_DIR` | `exports` | Onde o worker grava os CSV de consultas pesadas e de onde a API os serve |
| `EXPORT_TTL_SECONDS` | `86400` | Validade de cada arquivo exportado (24 h) |

## Os dois processos

Ambos importam `main.py`; o `PROCESS_ROLE` decide qual metade é construída. Sem essa variável, o
import de um dos dois montaria a aplicação inteira duas vezes (conexões dobradas).

### API

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

Stateless: o catálogo vive em memória e é invalidado por pub/sub. Pode escalar horizontalmente atrás
de um load balancer sem mudança de código. No boot, cada réplica:

1. Lê as versões ativas de `catalog_versions` e monta o `Catalog` em memória
2. Constrói um par de engines (leve/pesado) por `connection_ref`
3. Assina `catalog:invalidate` no Redis, numa task de fundo presa ao `lifespan`

### Worker

```bash
PROCESS_ROLE=worker arq main.WorkerSettings
```

Consome a fila de consultas pesadas, executa no **pool pesado** do datasource correspondente e grava
o resultado sob o `query_id`. Escala independentemente da API — se a fila cresce, suba mais workers.

Além disso, para cada job concluído ele **grava um CSV em `EXPORT_DIR`** (seção 2.4a do contrato), que
a API entrega em `GET /v1/query/{query_id}/download`. Um cron interno do próprio worker varre os
arquivos vencidos de hora em hora e no boot.

> **`EXPORT_DIR` precisa ser o mesmo caminho para a API e para o worker** — mesmo host, ou um volume
> compartilhado. É a limitação do adapter de filesystem: num deploy multi-nó sem volume comum, a API
> não acha o arquivo que outro nó escreveu, e o cliente recebe `404 export_not_found` para um export
> que existe. Trocar por um adapter de S3 fecha esse buraco sem mudar nada no contrato HTTP — a URL
> de download continua sendo a da própria API.

A varredura roda no worker, e não na API, de propósito: quem escreve os arquivos é quem os limpa, e
assim várias réplicas de API não disputam a mesma varredura.

## Publicação do catálogo

Git nunca é lido em tempo de execução; o banco nunca é editado à mão. A tabela é uma cópia
materializada, atualizada só pelo pipeline.

### Pelo script (CI)

```bash
GIT_SHA=$(git rev-parse HEAD) uv run python scripts/publish_catalog.py
```

Saída por schema, com código de retorno `1` se algum falhar:

```
inalterado estoque — hash de conteúdo idêntico ao da versão ativa
publicado  vendas @ c41b5575d796 (sem inspeção de datasource: vendas_detalhado)
```

Exemplo de workflow:

```yaml
name: Publicar catálogo
on:
  push:
    branches: [main]

jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v5
      - run: uv sync
      - run: uv run python scripts/publish_catalog.py
        env:
          GIT_SHA: ${{ github.sha }}
          CATALOG_DB_URL: ${{ secrets.CATALOG_DB_URL }}
          REDIS_URL: ${{ secrets.REDIS_URL }}
          INTERNAL_TOKEN: ${{ secrets.INTERNAL_TOKEN }}
          DW_VENDAS_PG_URL: ${{ secrets.DW_VENDAS_PG_URL }}
          DW_VENDAS_ORACLE_URL: ${{ secrets.DW_VENDAS_ORACLE_URL }}
          ES_EVENTOS_URL: ${{ secrets.ES_EVENTOS_URL }}
          APP_ESTOQUE_URL: ${{ secrets.APP_ESTOQUE_URL }}
```

### Pelo endpoint interno

```bash
curl -X POST localhost:8000/internal/catalog/publish \
  -H "X-Internal-Token: $INTERNAL_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"data": { ...schema em JSON... }, "git_sha": "abc123", "published_by": "eduardo"}'
```

`POST /internal/catalog/reload` força a recarga do catálogo em memória **desta instância** — o
caminho automático entre réplicas é o pub/sub, este é o manual.

### Rollback

Cada publicação insere uma linha nova; nada é sobrescrito. Para voltar uma versão, reative a linha
anterior (`is_active`) — o índice parcial `ux_catalog_active` garante que só uma fique ativa por
schema — e emita a invalidação:

```bash
redis-cli PUBLISH catalog:invalidate vendas
```

## Observabilidade

```bash
curl -s localhost:8000/internal/observability -H "X-Internal-Token: $INTERNAL_TOKEN" | jq
```

```json
{
  "cache": { "hits": 120, "misses": 30, "hit_rate": 0.8 },
  "heavy_queue": { "depth": 3 }
}
```

Os contadores de cache são acumulados no Redis (chaves `cache:hits` / `cache:misses`), então
sobrevivem ao restart de uma réplica e são compartilhados entre todas.

### Consultas lentas

Toda consulta que passa de `SLOW_QUERY_THRESHOLD_MS` gera um `WARNING`, tanto no caminho síncrono
quanto no worker:

```
WARNING consulta lenta: query_id=q_8f2a1c schema=vendas dataset=vendas_detalhado
        execution_ms=7431 row_count=1204
```

`meta.dataset_used` na resposta permite correlacionar: qual fonte física atendeu cada requisição,
sem que o cliente precise declarar nada.

### O que observar

| Sinal | Onde | Sintoma |
|---|---|---|
| `hit_rate` caindo | `/internal/observability` | Cache mal dimensionado ou TTL curto demais |
| `depth` crescendo | `/internal/observability` | Workers insuficientes — `429` quando bater `MAX_HEAVY_QUEUE_DEPTH` |
| `consulta lenta` frequente | Logs | Dataset mal escolhido, ou falta de dataset agregado no catálogo |
| `429 rate_limited` | Logs de acesso | Limite por cliente ou backpressure de fila |
| `404 export_not_found` em consulta concluída | Logs de acesso | `EXPORT_DIR` diferente entre API e worker, ou arquivo já vencido |
| `falha ao exportar o resultado` | Logs do worker | Disco cheio ou `EXPORT_DIR` sem permissão de escrita — o job conclui, só não deixa arquivo |

## Rate limiting

Dois limites independentes, ambos por cliente e por janela fixa no Redis:

| Limite | Onde incide | Env vars |
|---|---|---|
| **Geral** | Toda requisição a `/v1/query` | `REQUEST_RATE_LIMIT`, `REQUEST_RATE_LIMIT_WINDOW_SECONDS` |
| **Consultas pesadas** | Só quando a consulta seria enfileirada | `HEAVY_QUERY_RATE_LIMIT`, `HEAVY_QUERY_RATE_LIMIT_WINDOW_SECONDS` |

Além deles, `MAX_HEAVY_QUEUE_DEPTH` é um **backpressure global**: com a fila no teto, nenhuma
consulta pesada nova é aceita, independente de qual cliente pediu.

O cliente é identificado pelo header `X-Api-Key`; sem ele, pelo IP do socket (`ip:203.0.113.7`).
Omitir o header não escapa do limite.

`GET /v1/query/{query_id}` fica fora do rate limiting — é leitura de status, não execução.

## Ajuste de pools

A matriz de pools é **(datasource × leve/pesado)**, não só leve/pesado. Cada `connection_ref` ganha
seus dois pools, e eles nunca são compartilhados: uma consulta pesada segurando conexões não atrasa
consultas leves, e um datasource lento não esgota conexões destinadas a outro.

Dimensione `LIGHT_POOL_SIZE` pela concorrência esperada no caminho síncrono e `HEAVY_POOL_SIZE` pelo
que o banco aguenta de consultas caras simultâneas — lembrando que o total é multiplicado pelo
número de datasources **e** de réplicas.

Recomendações específicas por engine (Resource Manager no Oracle, `work_mem` e `statement_timeout`
no Postgres, `search.max_buckets` no Elasticsearch) estão em [`escalabilidade.md`](escalabilidade.md).

## Problemas comuns

| Erro no boot | Causa |
|---|---|
| `ConfigError: Variável de ambiente obrigatória ausente: X` | Falta uma das três obrigatórias, ou um `connection_ref` do catálogo ativo |
| `ModuleNotFoundError: oracledb` | Dependências desatualizadas — rode `uv sync` |
| Catálogo vazio, `/v1/catalog` devolve `[]` | Nada publicado ainda — rode `scripts/publish_catalog.py` |

| Erro na publicação | Causa |
|---|---|
| `invalid_catalog` com lista de campos | Colunas do `mapping` não existem no datasource |
| `Campo obrigatório ausente no catálogo` | YAML malformado — falta `schema`, `version` ou `datasource` |
| Todos os datasets "sem inspeção" | Inspetores não construídos — confira as env vars dos `connection_ref` |
