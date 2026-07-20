import logging

import pytest

from ingestion.base.ficheiro.watermark import Watermark


@pytest.fixture
def watermark(tmp_path) -> Watermark:
    w = Watermark(
        nome_ficheiro="teste_watermark.csv",
        colunas_ficheiro=["ultimo_recurso", "extraido_em"],
        nome_alteracoes="teste_alteracoes.csv",
        colunas_alteracoes=["view", "periodo"],
        nome_segredo="segredo-fake",
        logger=logging.getLogger("teste"),
    )
    w.DIRETORIO_WATERMARKS = tmp_path
    w.DIRETORIO_ALTERACOES = tmp_path
    return w


def test_registrar_entrada_sobrescreve_a_anterior(watermark: Watermark):
    # GIVEN
    ficheiro: dict[str, dict] = {}
    watermark.registrar_entrada(ficheiro, "VW_FT_PNCP_COMPRA", "2021-12-01", b"conteudo")

    # WHEN: registra um recurso novo
    watermark.registrar_entrada(ficheiro, "VW_FT_PNCP_COMPRA", "2021-12-02", b"conteudo")

    # THEN: só sobra a última entrada
    assert len(ficheiro) == 1
    assert ficheiro["ultimo_recurso"]["ultimo_recurso"] == "VW_FT_PNCP_COMPRA:2021-12-02"


def test_salvar_e_carregar_faz_roundtrip(watermark: Watermark):
    # GIVEN
    ficheiro: dict[str, dict] = {}
    watermark.registrar_entrada(ficheiro, "VW_FT_PNCP_COMPRA", "2021-12-01", b"conteudo")

    # WHEN
    watermark.salvar(ficheiro)
    carregado = watermark.carregar()

    # THEN
    assert carregado["ultimo_recurso"]["ultimo_recurso"] == "VW_FT_PNCP_COMPRA:2021-12-01"


def test_carregar_sem_arquivo_retorna_vazio(watermark: Watermark):
    # WHEN / THEN
    assert watermark.carregar() == {}


def test_salvar_vazio_grava_so_o_cabecalho(watermark: Watermark):
    # WHEN
    watermark.salvar({})

    # THEN
    linhas = watermark.caminho_ficheiro().read_text(encoding="utf-8").strip().splitlines()
    assert len(linhas) == 1
