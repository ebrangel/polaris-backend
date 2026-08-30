"""Negociação do formato de saída de `/v1/query` (seção 2.3a).

Módulo puro (sem FastAPI), como `query_params.py`: recebe o valor de `?format=` e o
header `Accept` já lidos e devolve o formato escolhido, para poder ser testado sem
subir aplicação.

**O formato não entra no `QueryRequest`.** `query_id` é o hash da requisição
(`QueryRequest.fingerprint`) e serve de chave de cache e de identificador do job
assíncrono (seção 3); se o formato fizesse parte dela, a mesma consulta pedida em JSON
e em CSV viraria dois `query_id` distintos — cache duplicado, execução duplicada no
banco e um `poll_url` por formato, tudo por uma diferença que não muda uma vírgula do
SQL gerado. Formato é representação do resultado, não parte da consulta.

**Por que dois mecanismos.** O `Accept` é o mecanismo correto do HTTP e é o que um
cliente programático usa. O `?format=` existe porque o caso de uso real do CSV é um
link de dashboard que o usuário clica para baixar — e navegador manda
`Accept: text/html,...,*/*;q=0.8`, de modo que negociação por header sozinha nunca
entregaria CSV nesse fluxo. Daí a precedência: `?format=` explícito > `Accept` > JSON.

**Assimetria proposital no tratamento de erro**: um `?format=` desconhecido é engano do
cliente e vira `422 invalid_format`; um `Accept` que não cita nada que a API produz cai
em JSON em vez de `406`. Content negotiation estrita devolveria 406, mas headers
`Accept` de navegador e de proxy são ruidosos demais para valer uma recusa.
"""

from enum import Enum

#: Media type completo da resposta CSV. `header=present` é o parâmetro do `text/csv` da
#: RFC 4180 (§3) que declara que a primeira linha traz os nomes das colunas.
CSV_MEDIA_TYPE = "text/csv; charset=utf-8; header=present"

JSON_MEDIA_TYPE = "application/json"


class OutputFormat(Enum):
    """Formato de saída pedido pelo cliente — conceito da borda HTTP, nunca do domínio.

    Acrescentar NDJSON ou Parquet no futuro é somar um membro aqui, um presenter e uma
    entrada no dispatch de `routers/query.py`.
    """

    JSON = "json"
    CSV = "csv"


#: Media type "nu" (sem parâmetros) → formato, para casar com o que vem no `Accept`.
_FORMAT_BY_MEDIA_TYPE = {
    "application/json": OutputFormat.JSON,
    "text/csv": OutputFormat.CSV,
}

_FORMAT_BY_NAME = {output_format.value: output_format for output_format in OutputFormat}


class UnsupportedFormatError(Exception):
    """`?format=` com um valor que a API não produz.

    Não é um `DomainError`: o domínio não conhece formato de saída. Tem handler próprio
    em `errors.py`, que a encaixa no mesmo envelope `problem+json` da seção 2.5.
    """

    def __init__(self, requested: str) -> None:
        self.requested = requested
        self.supported = tuple(_FORMAT_BY_NAME)
        super().__init__(f"Formato de saída não suportado: '{requested}'.")


def _parse_accept(header: str) -> list[str]:
    """`Accept` → media ranges com `q > 0`, do mais para o menos preferido.

    Empates de `q` preservam a ordem de declaração (`sort` é estável), que é a
    desempate usual na prática.
    """
    entries: list[tuple[float, int, str]] = []
    for position, part in enumerate(header.split(",")):
        segments = [segment.strip() for segment in part.split(";") if segment.strip()]
        if not segments:
            continue
        quality = 1.0
        for parameter in segments[1:]:
            name, _, value = parameter.partition("=")
            if name.strip().lower() == "q":
                try:
                    quality = float(value.strip())
                except ValueError:
                    quality = 0.0
        if quality > 0:
            entries.append((quality, position, segments[0].lower()))

    entries.sort(key=lambda entry: (-entry[0], entry[1]))
    return [media_range for _, _, media_range in entries]


def _from_accept(header: str | None) -> OutputFormat:
    if not header:
        return OutputFormat.JSON

    for media_range in _parse_accept(header):
        matched = _FORMAT_BY_MEDIA_TYPE.get(media_range)
        if matched is not None:
            return matched
        if media_range in ("*/*", "application/*"):
            return OutputFormat.JSON
        if media_range == "text/*":
            # CSV é o único `text/*` que a API produz.
            return OutputFormat.CSV
    return OutputFormat.JSON


def resolve_output_format(
    format_param: str | None, accept_header: str | None
) -> OutputFormat:
    """Formato da resposta: `?format=` explícito > `Accept` > JSON por omissão."""
    if format_param is not None and format_param.strip():
        requested = format_param.strip().lower()
        matched = _FORMAT_BY_NAME.get(requested)
        if matched is None:
            raise UnsupportedFormatError(format_param.strip())
        return matched
    return _from_accept(accept_header)
