"""Grupo B — download (HTTP 100% mockado)."""

import duckdb
import pytest
import requests
from apoio_delog import CSV_PADRAO, FakeResposta, FakeSessao, caminho_saida, executar, zip_bytes


def test_download_ok_cria_parquet(modulo):
    sessao = FakeSessao.com_csv(CSV_PADRAO)
    assert executar(modulo, sessao) == "criado"
    assert caminho_saida().is_file()
    url, kwargs = sessao.chamadas[0]
    assert kwargs["stream"] is True
    assert kwargs["timeout"] == (15, 120)


def test_404_indisponivel_sem_criar_arquivo(modulo):
    assert executar(modulo, FakeSessao.status(404)) == "indisponivel"
    assert not caminho_saida().exists()


@pytest.mark.parametrize("codigo", [500, 502, 503, 403, 401])
def test_status_de_erro_retorna_erro(modulo, codigo):
    assert executar(modulo, FakeSessao.status(codigo)) == "erro"
    assert not caminho_saida().exists()


@pytest.mark.parametrize("exc", [requests.Timeout("t"), requests.ConnectTimeout("ct"), requests.ReadTimeout("rt")])
def test_timeout_retorna_erro(modulo, exc):
    assert executar(modulo, FakeSessao.levantando(exc)) == "erro"
    assert not caminho_saida().exists()


def test_conexao_recusada_retorna_erro(modulo):
    assert executar(modulo, FakeSessao.levantando(requests.ConnectionError("recusada"))) == "erro"


@pytest.mark.parametrize(
    "exc", [requests.ConnectionError("reset"), requests.exceptions.ChunkedEncodingError("cortou")]
)
def test_conexao_interrompida_no_meio_do_stream(modulo, exc):
    sessao = FakeSessao(lambda url, **kw: FakeResposta(200, zip_bytes({"a.csv": CSV_PADRAO}), erro_no_stream=exc))
    assert executar(modulo, sessao) == "erro"
    assert not caminho_saida().exists()


def test_zip_truncado_retorna_erro(modulo):
    completo = zip_bytes({"a.csv": CSV_PADRAO})
    assert executar(modulo, FakeSessao.com_zip(completo[: len(completo) // 2])) == "erro"
    assert not caminho_saida().exists()


@pytest.mark.parametrize("conteudo", [b"isto nao e um zip", b"", b"<html>502 Bad Gateway</html>"])
def test_zip_invalido_retorna_erro(modulo, conteudo):
    assert executar(modulo, FakeSessao.com_zip(conteudo)) == "erro"
    assert not caminho_saida().exists()


@pytest.mark.parametrize(
    "arquivos",
    [{}, {"a.csv": CSV_PADRAO, "b.csv": CSV_PADRAO}, {"leiame.txt": b"x"}],
    ids=["zip_vazio", "dois_csv", "sem_csv"],
)
def test_zip_sem_exatamente_um_csv_retorna_erro(modulo, arquivos):
    assert executar(modulo, FakeSessao.com_zip(zip_bytes(arquivos))) == "erro"
    assert not caminho_saida().exists()


def test_arquivo_local_existente_nao_baixa_de_novo(modulo):
    destino = caminho_saida()
    destino.parent.mkdir(parents=True)

    duckdb.sql("SELECT 1 AS id").write_parquet(str(destino))
    conteudo_original = destino.read_bytes()

    sessao = FakeSessao.com_csv(CSV_PADRAO)

    assert executar(modulo, sessao) == "existente"
    assert sessao.chamadas == []
    assert destino.read_bytes() == conteudo_original


def test_caracterizacao_500_nao_tem_retry(modulo):
    """Comportamento ATUAL: uma única tentativa (o extract legado tem 3). Ver relatório."""
    sessao = FakeSessao.status(500)
    executar(modulo, sessao)
    assert len(sessao.chamadas) == 1


def test_temporarios_sao_removidos_apos_falha(modulo, tmp_sistema):
    executar(modulo, FakeSessao.com_zip(b"lixo"))
    assert list(tmp_sistema.glob("colibri_delog_*")) == []
