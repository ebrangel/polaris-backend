"""Port do export de resultados (seções 2.3a e 2.4a).

O worker é o processo que lê o cursor; este port existe para que ele **grave o resultado
como artefato baixável enquanto lê**, sem que nenhum processo precise segurar o resultado
inteiro na memória, e para que o artefato sobreviva ao TTL da entrada no Redis.

**Por que o port recebe domínio e não bytes já formatados.** Quem chama é o use case
`RunQueuedQuery`, em `application/`, que não pode importar `adapters/` (regra de
dependência do CLAUDE.md, verificada por `tests/test_layer_purity.py`) — e CSV e JSON são
formato, assunto de adapter. Por isso o port é um *exportador* (formata e persiste), e não
um *armazenamento*. Trocar o adapter de filesystem por um de S3 não muda nem este contrato
nem o contrato HTTP.

**Três arquivos por consulta** (`ExportKind`), porque cada um responde a uma pergunta
diferente e nenhum responde às três:

- `CSV` — o download da seção 2.4a e o `?format=csv` da 2.3a;
- `JSONL` — uma lista JSON por linha, de onde a API transmite o corpo JSON da seção 2.3
  sem materializá-lo. Não dá para derivar isso do CSV: a RFC 4180 não distingue `NULL` de
  string vazia, então a volta perderia informação;
- `META` — colunas e `meta`, gravado **por último**, o que faz sua existência ser o sinal
  de que o export está completo. É também o que deixa `GET /v1/query/{query_id}` responder
  depois de o resultado retido do `arq` ter expirado.
"""

from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Protocol, runtime_checkable

from application.ports.row_sink import RowSink
from domain.models import Column, QueryResult


class ExportKind(str, Enum):
    """Qual dos artefatos de uma consulta se quer ler."""

    CSV = "csv"
    JSONL = "jsonl"
    META = "meta"


@dataclass(frozen=True, slots=True)
class ExportMetadata:
    """O que a API precisa saber sobre um export sem abrir o arquivo.

    `size_bytes` vira `Content-Length` na resposta de download; `expires_at` vira o
    `download_expires_at` do corpo da seção 2.4, para o cliente saber até quando o link
    vale sem ter que tentar baixar. Ambos se referem ao `ExportKind` pedido no `stat`.
    """

    query_id: str
    kind: ExportKind
    size_bytes: int
    created_at: datetime
    expires_at: datetime


@runtime_checkable
class ResultExporter(Protocol):
    """Grava resultados como arquivos e os serve de volta por `query_id`."""

    async def open_writer(
        self, query_id: str, columns: tuple[Column, ...], dataset_used: str
    ) -> RowSink:
        """Abre os artefatos desta consulta e devolve o sink, já pronto para receber.

        Substitui um export anterior do mesmo `query_id` — mas só no `close()` do sink,
        de modo que um download concorrente enxergue sempre um arquivo inteiro: o
        anterior, ou o novo.
        """
        ...

    async def stat(
        self, query_id: str, kind: ExportKind = ExportKind.CSV
    ) -> ExportMetadata | None:
        """Metadados do artefato, ou `None` se não existir **ou já ter expirado**.

        O TTL é autoritativo aqui, e não na varredura: um arquivo vencido deixa de ser
        servido no instante em que vence, independentemente de quando a limpeza
        periódica (`purge_expired`) rodar por cima dele.
        """
        ...

    async def read_result(self, query_id: str) -> QueryResult | None:
        """Reconstrói o descritor (colunas e `meta`) a partir do arquivo `META`.

        `rows` vem `None`: o descritor descreve o resultado, as linhas continuam nos
        outros dois artefatos. Devolve `None` se o export não existir ou tiver expirado.
        """
        ...

    async def open(
        self, query_id: str, kind: ExportKind = ExportKind.CSV
    ) -> AsyncIterator[bytes]:
        """Conteúdo do artefato em blocos, para a API repassar sem materializar.

        Chamado depois de um `stat()` que encontrou o arquivo. A implementação deve
        garantir o acesso ao conteúdo **no momento da chamada** (e não a cada bloco), de
        modo que uma limpeza concorrente não trunque um download em andamento; se o
        export sumir entre o `stat()` e o `open()`, levanta `FileNotFoundError`.
        """
        ...

    async def purge_expired(self) -> int:
        """Remove os exports vencidos e devolve quantas consultas foram apagadas.

        Chamado pelo cron do worker (`adapters/queue/tasks.py`). Conta consultas, não
        arquivos: os três artefatos de um `query_id` vencem juntos e são removidos como
        um conjunto. Idempotente: rodar duas vezes seguidas não é erro, a segunda só
        devolve `0`.
        """
        ...
