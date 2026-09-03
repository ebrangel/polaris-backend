"""`FanOutSink` — distribuir cada bloco para vários destinos, tolerando queda de um.

O contrato que estes testes protegem: quando o executor já leu o cursor, o caro da
consulta foi pago. Perder o cache ou o arquivo é perder uma otimização; derrubar o job
por causa disso seria trocar uma resposta pronta por um erro.
"""

import logging

import pytest
from fakes import CollectingRowSink

from application.ports.row_sink import StreamedResult
from application.use_cases._fan_out import FanOutSink

RESULTADO = StreamedResult(row_count=3, total_rows=3, execution_ms=42)


async def test_replica_cada_bloco_em_todos_os_destinos():
    a, b = CollectingRowSink(), CollectingRowSink()
    fan = FanOutSink([a, b])

    await fan.write([("SP", 1)])
    await fan.write([("RJ", 2), ("MG", 3)])
    await fan.close(RESULTADO)

    # O recorte em blocos é preservado: o fan-out repassa, não reagrupa.
    assert a.chunks == b.chunks == [[("SP", 1)], [("RJ", 2), ("MG", 3)]]
    assert a.closed_with == b.closed_with == RESULTADO


async def test_destino_que_falha_na_escrita_sai_e_os_outros_seguem(caplog):
    bom = CollectingRowSink()
    ruim = CollectingRowSink(raises=ConnectionError("redis fora do ar"))
    fan = FanOutSink([bom, ruim])

    with caplog.at_level(logging.WARNING):
        await fan.write([("SP", 1)])
        await fan.write([("RJ", 2)])
        await fan.close(RESULTADO)

    assert bom.rows == [("SP", 1), ("RJ", 2)]
    assert bom.closed_with == RESULTADO
    assert fan.degraded is True
    assert "os demais seguem" in caplog.text


async def test_destino_que_falha_e_abortado_para_nao_deixar_lixo(caplog):
    """Sem isso, cada falha deixaria um temporário órfão no `export_dir`."""
    ruim = CollectingRowSink(raises=OSError("disco cheio"))
    fan = FanOutSink([ruim])

    with caplog.at_level(logging.WARNING):
        await fan.write([("SP", 1)])

    assert ruim.aborted is True


async def test_destino_so_e_tentado_uma_vez_depois_de_falhar():
    ruim = CollectingRowSink(raises=ConnectionError("caiu"))
    fan = FanOutSink([ruim])

    await fan.write([("SP", 1)])
    await fan.write([("RJ", 2)])

    # O sink saiu da lista na primeira falha; a segunda escrita não o alcança, então o
    # log não vira uma linha por bloco num resultado de milhares de linhas.
    assert ruim.aborted is True


async def test_falha_no_close_de_um_nao_impede_o_close_do_outro(caplog):
    class _FalhaNoClose(CollectingRowSink):
        async def close(self, result):
            raise OSError("falhou ao renomear")

    bom = CollectingRowSink()
    fan = FanOutSink([_FalhaNoClose(), bom])

    with caplog.at_level(logging.WARNING):
        await fan.close(RESULTADO)

    assert bom.closed_with == RESULTADO


async def test_abort_nao_levanta_mesmo_com_destino_quebrado(caplog):
    """`abort` roda em caminho de erro: uma segunda exceção esconderia a primeira, que é
    a que interessa a quem for investigar."""

    class _FalhaNoAbort(CollectingRowSink):
        async def abort(self):
            raise OSError("nem abortar deu")

    fan = FanOutSink([_FalhaNoAbort(), CollectingRowSink()])

    with caplog.at_level(logging.WARNING):
        await fan.abort()  # não levanta

    assert "falha ao abortar" in caplog.text


async def test_sem_destino_algum_nao_falha():
    """Worker sem cache nem export configurados — a consulta ainda tem de rodar."""
    fan = FanOutSink([])

    await fan.write([("SP", 1)])
    await fan.close(RESULTADO)

    assert fan.degraded is False
