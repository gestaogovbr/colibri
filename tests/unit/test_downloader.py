import requests

from ingestion.base.downloader import Downloader


def _resposta(status_code: int = 200, conteudo: bytes = b"") -> requests.Response:
    resposta = requests.Response()
    resposta.status_code = status_code
    resposta._content = conteudo
    return resposta


def test_baixar_arquivo_retorna_o_conteudo(monkeypatch):
    # GIVEN
    d = Downloader()
    monkeypatch.setattr(d._session, "get", lambda url, timeout: _resposta(200, b"dados"))

    # WHEN
    resultado = d.baixar_arquivo("https://exemplo.com/arquivo")

    # THEN
    assert resultado == b"dados"


def test_baixar_arquivo_404_retorna_none(monkeypatch):
    # GIVEN
    d = Downloader()
    monkeypatch.setattr(d._session, "get", lambda url, timeout: _resposta(404))

    # WHEN / THEN
    assert d.baixar_arquivo("https://exemplo.com/inexistente") is None


def test_baixar_arquivo_desiste_apos_max_tentativas(monkeypatch):
    # GIVEN
    d = Downloader()
    d.PAUSA_BASE_SEGUNDOS = 0  # não espera de verdade no teste
    tentativas = []

    def get_com_timeout(url, timeout):
        tentativas.append(url)
        raise requests.exceptions.Timeout()

    monkeypatch.setattr(d._session, "get", get_com_timeout)

    # WHEN
    resultado = d.baixar_arquivo("https://exemplo.com/lento")

    # THEN
    assert resultado is None
    assert len(tentativas) == d.MAX_TENTATIVAS


def test_obter_tipo_conteudo_le_o_content_type(monkeypatch):
    # GIVEN
    d = Downloader()
    resposta = _resposta(200)
    resposta.headers["Content-Type"] = "application/json"
    monkeypatch.setattr(d._session, "head", lambda url, allow_redirects, timeout: resposta)

    # WHEN / THEN
    assert d.obter_tipo_conteudo("https://exemplo.com/arquivo.json") == "application/json"


def test_obter_tipo_conteudo_indisponivel_retorna_vazio(monkeypatch):
    # GIVEN
    d = Downloader()

    def head_com_erro(url, allow_redirects, timeout):
        raise requests.exceptions.ConnectionError()

    monkeypatch.setattr(d._session, "head", head_com_erro)

    # WHEN / THEN
    assert d.obter_tipo_conteudo("https://exemplo.com/indisponivel") == ""
