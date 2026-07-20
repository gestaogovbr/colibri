import logging

import pytest

from ingestion.base.ficheiro.manifesto import Manifesto


@pytest.fixture
def manifesto(tmp_path) -> Manifesto:
    m = Manifesto(
        nome_ficheiro="teste_manifesto.csv",
        colunas_ficheiro=["tabela", "periodo", "hash_sha256", "num_linhas", "tamanho_bytes", "extraido_em"],
        nome_alteracoes="teste_alteracoes.csv",
        colunas_alteracoes=["tabela", "periodo"],
        nome_segredo="segredo-fake",
        logger=logging.getLogger("teste"),
    )
    m.DIRETORIO_FICHEIROS = tmp_path
    m.DIRETORIO_ALTERACOES = tmp_path
    return m


def test_caminho_ficheiro_e_alteracoes(manifesto: Manifesto, tmp_path):
    # WHEN / THEN
    assert manifesto.caminho_ficheiro() == tmp_path / "teste_manifesto.csv"
    assert manifesto.caminho_alteracoes() == tmp_path / "teste_alteracoes.csv"


def test_registrar_entrada_calcula_hash_e_linhas(manifesto: Manifesto):
    # GIVEN
    ficheiro: dict[str, dict] = {}
    conteudo = b"id,nome\n1,Aninha\n2,Bruno\n"

    # WHEN
    manifesto.registrar_entrada(ficheiro, "catmats", "1", conteudo)

    # THEN
    entrada = ficheiro["catmats:1"]
    assert entrada["tabela"] == "catmats"
    assert entrada["periodo"] == "1"
    assert entrada["num_linhas"] == 2
    assert entrada["tamanho_bytes"] == len(conteudo)
    assert "hash_sha256" in entrada


def test_registrar_entrada_mantem_so_colunas_declaradas(manifesto: Manifesto):
    # GIVEN: "num_colunas" é calculado mas não está em colunas_ficheiro
    ficheiro: dict[str, dict] = {}

    # WHEN
    manifesto.registrar_entrada(ficheiro, "catmats", "1", b"id,nome\n1,Aninha\n")

    # THEN
    assert "num_colunas" not in ficheiro["catmats:1"]


def test_registrar_entrada_conteudo_nao_csv_fica_sem_linhas(manifesto: Manifesto):
    # GIVEN: bytes que não decodificam como texto (simula zip/xlsx)
    ficheiro: dict[str, dict] = {}
    conteudo = bytes([0xFF, 0xFE, 0x00, 0x01])

    # WHEN
    manifesto.registrar_entrada(ficheiro, "nfe_cgu", "2022-01", conteudo)

    # THEN
    assert ficheiro["nfe_cgu:2022-01"]["num_linhas"] is None


def test_salvar_e_carregar_faz_roundtrip(manifesto: Manifesto):
    # GIVEN
    ficheiro: dict[str, dict] = {}
    manifesto.registrar_entrada(ficheiro, "catmats", "1", b"id,nome\n1,Aninha\n")

    # WHEN
    manifesto.salvar(ficheiro)
    carregado = manifesto.carregar()

    # THEN: o CSV só guarda texto, então os valores voltam como string
    assert carregado.keys() == ficheiro.keys()
    assert carregado["catmats:1"]["hash_sha256"] == ficheiro["catmats:1"]["hash_sha256"]
    assert carregado["catmats:1"]["num_linhas"] == str(ficheiro["catmats:1"]["num_linhas"])


def test_carregar_sem_arquivo_retorna_vazio(manifesto: Manifesto):
    # WHEN / THEN
    assert manifesto.carregar() == {}


def test_salvar_periodicamente_respeita_o_intervalo(manifesto: Manifesto):
    # GIVEN
    manifesto.SALVAR_A_CADA = 2
    ficheiro: dict[str, dict] = {}
    manifesto.registrar_entrada(ficheiro, "catmats", "1", b"id,nome\n1,Aninha\n")

    # WHEN / THEN
    manifesto.salvar_periodicamente(1, ficheiro)
    assert not manifesto.caminho_ficheiro().exists()

    manifesto.salvar_periodicamente(2, ficheiro)
    assert manifesto.caminho_ficheiro().exists()


def test_resetar_apaga_ficheiro_e_alteracoes(manifesto: Manifesto):
    # GIVEN
    ficheiro: dict[str, dict] = {}
    manifesto.registrar_entrada(ficheiro, "catmats", "1", b"id,nome\n1,Aninha\n")
    manifesto.salvar(ficheiro)
    manifesto.salvar_alteracoes([("catmats", "1")])

    # WHEN
    manifesto.resetar()

    # THEN
    assert not manifesto.caminho_ficheiro().exists()
    assert not manifesto.caminho_alteracoes().exists()
