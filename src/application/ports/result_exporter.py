"""Port do export de resultados de consultas pesadas (seção 2.4a).

Uma consulta pesada já roda no worker, que é o processo com orçamento de memória para
ela. Este port existe para que ele **grave o resultado como arquivo baixável** antes de
devolver o job: assim o processo da API nunca precisa segurar o resultado inteiro para
entregar um export, e o arquivo sobrevive ao TTL da entrada no Redis.

**Por que o port recebe `QueryResult` e não linhas de CSV já formatadas.** Quem chama é
o use case `RunQueuedQuery`, em `application/`, que não pode importar `adapters/` (regra
de dependência do CLAUDE.md, verificada por `tests/test_layer_purity.py`) — e CSV é
formato, assunto de adapter. Por isso o port é um *exportador*, não um *armazenamento*:
ele decide o formato e persiste. Trocar o adapter de filesystem por um de S3 não muda
nem este contrato nem o contrato HTTP.
"""

from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, runtime_checkable

from domain.models import QueryResult


@dataclass(frozen=True, slots=True)
class ExportMetadata:
    """O que a API precisa saber sobre um export sem abrir o arquivo.

    `size_bytes` vira `Content-Length` na resposta de download; `expires_at` vira o
    `download_expires_at` do corpo da seção 2.4, para o cliente saber até quando o link
    vale sem ter que tentar baixar.
    """

    query_id: str
    size_bytes: int
    created_at: datetime
    expires_at: datetime


@runtime_checkable
class ResultExporter(Protocol):
    """Grava resultados concluídos como arquivo e os serve de volta por `query_id`."""

    async def export(self, result: QueryResult) -> ExportMetadata:
        """Grava o resultado como arquivo baixável, substituindo um export anterior do
        mesmo `query_id`.

        Só faz sentido para `status=completed` — implementações devem recusar os demais,
        como `CacheGateway.set` já faz.
        """
        ...

    async def stat(self, query_id: str) -> ExportMetadata | None:
        """Metadados do export, ou `None` se não existir **ou já ter expirado**.

        O TTL é autoritativo aqui, e não na varredura: um arquivo vencido deixa de ser
        servido no instante em que vence, independentemente de quando a limpeza
        periódica (`purge_expired`) rodar por cima dele.
        """
        ...

    async def open(self, query_id: str) -> AsyncIterator[bytes]:
        """Conteúdo do export em blocos, para a API repassar sem materializar.

        Chamado depois de um `stat()` que encontrou o arquivo. A implementação deve
        garantir o acesso ao conteúdo **no momento da chamada** (e não a cada bloco), de
        modo que uma limpeza concorrente não trunque um download em andamento; se o
        export sumir entre o `stat()` e o `open()`, levanta `FileNotFoundError`.
        """
        ...

    async def purge_expired(self) -> int:
        """Remove os exports vencidos e devolve quantos foram apagados.

        Chamado pelo cron do worker (`adapters/queue/tasks.py`). Idempotente: rodar duas
        vezes seguidas não é erro, a segunda só devolve `0`.
        """
        ...
