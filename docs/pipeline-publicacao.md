# Pipeline de publicação incremental do catálogo

## Princípio

Git nunca é lido em tempo de execução; o banco nunca é editado à mão. O banco é uma cópia materializada, atualizada apenas por um pipeline automatizado ao dar merge na branch principal.

## Tabela no banco (Postgres)

```sql
CREATE TABLE catalog_versions (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    schema_name   VARCHAR(100) NOT NULL,
    git_sha       VARCHAR(40)  NOT NULL,
    content       TEXT         NOT NULL,     -- JSON compilado (inclui a lista de datasets)
    content_hash  VARCHAR(64)  NOT NULL,
    published_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_by  VARCHAR(100),
    is_active     BOOLEAN      NOT NULL DEFAULT false
);

CREATE UNIQUE INDEX ux_catalog_active
    ON catalog_versions (schema_name)
    WHERE is_active = true;
```

Um `schema_name` pode ter múltiplos datasets, cada um com seu próprio datasource — por isso não há uma coluna `datasource_type` única na tabela; essa informação fica dentro de `content`, por dataset. Esse banco Postgres do catálogo é infraestrutura própria — separado de qualquer datasource analítico que também seja Postgres.

Cada publicação insere uma nova linha (nunca `UPDATE` no conteúdo); a anterior é desativada na mesma transação. Isso dá histórico completo e rollback trivial (reativar uma versão anterior).

## Lógica de publicação incremental

Não usar `git diff` entre commits (quebra com squash/rebase/force-push). Comparar hash de conteúdo compilado contra o que já está ativo no banco:

```python
for file in list_schema_files("catalog/schemas/*.yaml"):
    schema_name = extract_schema_name(file)
    compiled = compile_yaml(file)
    new_hash = sha256(compiled)

    active = get_active_version(schema_name)

    if active and active.content_hash == new_hash:
        continue  # nada mudou, pula validação e publicação

    # confere tabelas/colunas (ou índice/campos, no caso de elasticsearch) em
    # cada datasource referenciado pelos datasets dentro de compiled["datasets"]
    validate_semantics(compiled)
    publish_new_version(schema_name, compiled, new_hash, git_sha=current_sha)
```

## Dimensões compartilhadas entre schemas

Se dimensões forem fatoradas em arquivos separados e reaproveitadas por múltiplos fatos, uma mudança nesse arquivo precisa disparar republicação de todos os schemas dependentes. Duas opções:
1. Manifesto de dependências (`dependencies.yaml`) mapeando arquivo compartilhado → schemas que o referenciam
2. Manter cada schema autocontido (duplicando definição de dimensão) — mais simples, recomendado enquanto o catálogo for pequeno

## Invalidação de cache após publicação

Ao publicar, emitir evento em um tópico Redis Pub/Sub (`catalog:invalidate`) para que todas as instâncias da API recarreguem o catálogo em memória imediatamente, em vez de depender de polling periódico.
