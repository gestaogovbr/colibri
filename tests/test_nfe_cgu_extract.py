"""Testes de ingestion/nfe_cgu/extract.py: tipos gravados no Parquet e decisão de reconverter (#98)."""

import hashlib

import duckdb
import pytest

from ingestion.nfe_cgu import extract as ex

CHAVE = "35240112345678000199550010000012341987654321"


@pytest.fixture
def ler_parquet(tmp_path):
    """Converte o CSV com csv_para_parquet e devolve o Parquet gravado, como o dbt o leria"""

    def _ler(conteudo: bytes) -> duckdb.DuckDBPyRelation:
        caminho = tmp_path / f"{len(list(tmp_path.iterdir()))}.parquet"
        caminho.write_bytes(ex.csv_para_parquet(conteudo))
        return duckdb.from_parquet(str(caminho))

    return _ler


def test_chave_de_acesso_chega_inteira_como_texto(ler_parquet):
    csv = f'"CHAVE DE ACESSO";"NÚMERO"\n"{CHAVE}";"1234"\n'.encode()
    rel = ler_parquet(csv)
    tipos = dict(zip(rel.columns, map(str, rel.types), strict=True))
    assert tipos["CHAVE DE ACESSO"] == "VARCHAR"
    assert rel.fetchall() == [(CHAVE, 1234)]


def test_ncm_vira_texto_e_demais_colunas_mantem_inferencia(ler_parquet):
    csv = f'"CHAVE DE ACESSO";"CÓDIGO NCM/SH";"CFOP"\n"{CHAVE}";"4032000";"5102"\n'.encode()
    rel = ler_parquet(csv)
    tipos = dict(zip(rel.columns, map(str, rel.types), strict=True))
    assert tipos == {"CHAVE DE ACESSO": "VARCHAR", "CÓDIGO NCM/SH": "VARCHAR", "CFOP": "BIGINT"}
    assert rel.fetchone()[1] == "4032000"


def test_layout_sem_colunas_texto_nao_quebra(ler_parquet):
    # eventos não tem NCM; uma tabela sem nenhuma das COLUNAS_TEXTO também tem que passar
    csv = b'"MODELO";"VALOR"\n"55";"10"\n'
    assert ler_parquet(csv).fetchall() == [(55, 10)]


def test_nomes_acentuados_vindos_do_latin1(ler_parquet):
    bruto = f'"CHAVE DE ACESSO";"CÓDIGO NCM/SH"\n"{CHAVE}";"84729040"\n'.encode("latin-1")
    rel = ler_parquet(bruto.decode("latin-1").encode("utf-8"))
    assert rel.fetchall() == [(CHAVE, "84729040")]


def _entrada(conteudo: bytes, versao: str | None) -> dict:
    e = {"hash_sha256": hashlib.sha256(conteudo).hexdigest()}
    if versao is not None:
        e["versao_conversao"] = versao
    return e


def test_esta_em_dia_exige_hash_e_versao_atuais():
    c = b"a;b\n1;2\n"
    assert ex.esta_em_dia(_entrada(c, ex.VERSAO_CONVERSAO), c)
    assert not ex.esta_em_dia(None, c)
    assert not ex.esta_em_dia(_entrada(b"outro", ex.VERSAO_CONVERSAO), c)


def test_manifesto_antigo_sem_versao_forca_reconversao(tmp_path):
    # manifesto gravado antes do #98: sem a coluna versao_conversao
    c = b"a;b\n1;2\n"
    antigo = tmp_path / "m.csv"
    antigo.write_text(
        "tabela,periodo,hash_sha256,num_linhas,tamanho_bytes,extraido_em\n"
        f"nf,2025-08,{hashlib.sha256(c).hexdigest()},1,8,2026-07-13T13:43:00\n",
        encoding="utf-8",
    )
    manifesto = ex.carregar_manifesto(antigo)
    assert not ex.esta_em_dia(manifesto["nf:2025-08"], c)

    # reconvertido, a entrada nova fica em dia e o manifesto regravado carrega a versão
    ex.registrar_entrada(manifesto, "nf", "2025-08", c)
    ex.salvar_manifesto(antigo, manifesto)
    relido = ex.carregar_manifesto(antigo)
    assert relido["nf:2025-08"]["versao_conversao"] == ex.VERSAO_CONVERSAO
    assert ex.esta_em_dia(relido["nf:2025-08"], c)


def test_csv_real_com_aspas_e_ponto_e_virgula_no_texto(ler_parquet):
    csv = f'"CHAVE DE ACESSO";"DESCRIÇÃO"\n"{CHAVE}";"CANETA; AZUL"\n'.encode()
    assert ler_parquet(csv).fetchall() == [(CHAVE, "CANETA; AZUL")]


def test_tem_versao_antiga():
    assert not ex.tem_versao_antiga({})
    assert not ex.tem_versao_antiga({"nf:2025-08": {"versao_conversao": ex.VERSAO_CONVERSAO}})
    assert ex.tem_versao_antiga(
        {"nf:2025-08": {"versao_conversao": ex.VERSAO_CONVERSAO}, "nf:2025-09": {"versao_conversao": ""}}
    )
