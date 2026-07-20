from pathlib import Path

import pytest

from ingestion.base.tarefa import Tarefa


def test_tarefa_guarda_os_campos_passados():
    # GIVEN / WHEN
    tarefa = Tarefa(
        tabela="catmats",
        identificador="1",
        url="https://exemplo.com/1",
        chave_bucket=Path("catmats/1.parquet"),
        alteracoes=[("catmats", "1")],
    )

    # THEN
    assert tarefa.tabela == "catmats"
    assert tarefa.identificador == "1"
    assert tarefa.url == "https://exemplo.com/1"
    assert tarefa.chave_bucket == Path("catmats/1.parquet")
    assert tarefa.alteracoes == [("catmats", "1")]


def test_tarefa_e_imutavel():
    # GIVEN
    tarefa = Tarefa(
        tabela="nfe_cgu",
        identificador="2022-01",
        url="https://exemplo.com/zip",
        chave_bucket=Path("nfe_cgu/2022-01.parquet"),
        alteracoes=[],
    )

    # WHEN / THEN
    with pytest.raises(AttributeError):
        tarefa.tabela = "outra"
