"""Testes da decisão HEAD/ETag × GET em ingestion/pncp_comprasgov/extract.py.

Tudo é simulado: sessão HTTP falsa, bucket falso, conversão parquet falsa.
Nada toca rede, credenciais ou disco além do tmp_path do pytest.
"""

import hashlib
from datetime import date

import pytest
import requests

from ingestion.pncp_comprasgov import extract as ex

VIEW = "VW_FT_PNCP_COMPRA"
CHAVE = "2024-01-01"
URL = ex.construir_url_diario(VIEW, date(2024, 1, 1))
CAMINHO = ex.construir_caminho_diario(VIEW, date(2024, 1, 1))
BUCKET = "bucket-teste"

CONTEUDO = b"a,b\n1,2\n3,4\n"
CONTEUDO_NOVO = b"a,b\n1,2\n3,4\n5,6\n"
ETAG = '"6874a1b2-1f"'
ETAG_NOVO = '"6890ffff-2a"'


def sha(conteudo: bytes) -> str:
    return hashlib.sha256(conteudo).hexdigest()


class RespostaFake:
    def __init__(self, status_code=200, headers=None, content=b""):
        self.status_code = status_code
        self.headers = headers or {}
        self.content = content

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"HTTP {self.status_code}")


class SessaoFake:
    """Responde HEAD e GET com o que for configurado; uma exceção configurada é
    levantada em vez de retornada. Registra a ordem das chamadas."""

    def __init__(self, head=None, get=None):
        self._head = head
        self._get = get
        self.chamadas: list[str] = []

    def head(self, url, timeout=None):
        self.chamadas.append("HEAD")
        if isinstance(self._head, Exception):
            raise self._head
        return self._head

    def get(self, url, timeout=None):
        self.chamadas.append("GET")
        if isinstance(self._get, Exception):
            raise self._get
        return self._get


def resposta_ok(conteudo=CONTEUDO, etag=ETAG):
    return RespostaFake(
        200,
        {"ETag": etag, "Last-Modified": "Thu, 04 Jan 2024 10:00:00 GMT"},
        conteudo,
    )


def entrada(etag="", conteudo=CONTEUDO) -> dict:
    return {
        "view": VIEW,
        "data": CHAVE,
        "url": URL,
        "num_linhas": "2",
        "tamanho_bytes": str(len(conteudo)),
        "num_colunas": "2",
        "hash_sha256": sha(conteudo),
        "extraido_em": "2024-01-04T12:00:00",
        "etag": etag,
        "last_modified": "",
    }


@pytest.fixture
def ambiente(monkeypatch):
    """Isola processar_arquivo do mundo: parquet, upload e bucket são falsos."""
    estado = {"no_bucket": True, "uploads": []}
    monkeypatch.setattr(ex, "csv_para_parquet", lambda conteudo: b"PARQUET")
    monkeypatch.setattr(
        ex,
        "salvar_bytes_no_bucket",
        lambda conteudo, bucket, segredo, nome: estado["uploads"].append(nome),
    )
    monkeypatch.setattr(ex, "existe_no_bucket", lambda cliente, bucket, nome: estado["no_bucket"])
    monkeypatch.setattr(ex, "VERIFICACAO_COMPLETA", False)
    monkeypatch.setattr(ex.time, "sleep", lambda s: None)
    return estado


def processar(sessao, manifesto):
    return ex.processar_arquivo(sessao, VIEW, CHAVE, URL, CAMINHO, manifesto, BUCKET, cliente=object())


# Sem ETag no manifesto (primeira carga ou manifesto antigo): comportamento de sempre


def test_primeiro_download_registra_etag(ambiente):
    sessao = SessaoFake(get=resposta_ok())
    manifesto = {}

    assert processar(sessao, manifesto) == "baixado"
    assert sessao.chamadas == ["GET"]
    assert ambiente["uploads"] == [CAMINHO.relative_to(ex.DIRETORIO_RAIZ).with_suffix(".parquet").as_posix()]
    registro = manifesto[f"{VIEW}:{CHAVE}"]
    assert registro["etag"] == ETAG
    assert registro["last_modified"] == "Thu, 04 Jan 2024 10:00:00 GMT"
    assert registro["hash_sha256"] == sha(CONTEUDO)


def test_sem_etag_no_manifesto_faz_get_e_aprende_etag(ambiente):
    sessao = SessaoFake(get=resposta_ok())
    manifesto = {f"{VIEW}:{CHAVE}": entrada(etag="")}

    assert processar(sessao, manifesto) == "ignorado"
    assert sessao.chamadas == ["GET"]  # sem ETag conhecido não há o que comparar
    assert ambiente["uploads"] == []
    assert manifesto[f"{VIEW}:{CHAVE}"]["etag"] == ETAG  # próxima rodada pula o GET


def test_get_404_e_indisponivel(ambiente):
    sessao = SessaoFake(get=RespostaFake(404))

    assert processar(sessao, {}) == "indisponivel"
    assert sessao.chamadas == ["GET"]


# Com ETag no manifesto: o atalho


def test_etag_igual_e_objeto_no_bucket_pula_sem_baixar(ambiente):
    sessao = SessaoFake(head=resposta_ok(), get=resposta_ok())
    manifesto = {f"{VIEW}:{CHAVE}": entrada(etag=ETAG)}

    assert processar(sessao, manifesto) == "ignorado"
    assert sessao.chamadas == ["HEAD"]
    assert ambiente["uploads"] == []


def test_etag_igual_mas_objeto_sumiu_do_bucket_rebaixa(ambiente):
    ambiente["no_bucket"] = False
    sessao = SessaoFake(head=resposta_ok(), get=resposta_ok())
    manifesto = {f"{VIEW}:{CHAVE}": entrada(etag=ETAG)}

    assert processar(sessao, manifesto) == "atualizado"
    assert sessao.chamadas == ["HEAD", "GET"]
    assert len(ambiente["uploads"]) == 1


def test_etag_diferente_rebaixa_e_atualiza_manifesto(ambiente):
    sessao = SessaoFake(head=resposta_ok(etag=ETAG_NOVO), get=resposta_ok(CONTEUDO_NOVO, ETAG_NOVO))
    manifesto = {f"{VIEW}:{CHAVE}": entrada(etag=ETAG)}

    assert processar(sessao, manifesto) == "atualizado"
    assert sessao.chamadas == ["HEAD", "GET"]
    assert len(ambiente["uploads"]) == 1
    registro = manifesto[f"{VIEW}:{CHAVE}"]
    assert registro["etag"] == ETAG_NOVO
    assert registro["hash_sha256"] == sha(CONTEUDO_NOVO)
    assert registro["num_linhas"] == 3


def test_etag_mudou_mas_conteudo_igual_so_renova_etag(ambiente):
    # ETag do nginx = mtime + tamanho: um "touch" na fonte muda o ETag sem mudar
    # o conteúdo. Custa um download; o hash segura e o ETag novo é aprendido.
    sessao = SessaoFake(head=resposta_ok(etag=ETAG_NOVO), get=resposta_ok(CONTEUDO, ETAG_NOVO))
    manifesto = {f"{VIEW}:{CHAVE}": entrada(etag=ETAG)}

    assert processar(sessao, manifesto) == "ignorado"
    assert sessao.chamadas == ["HEAD", "GET"]
    assert ambiente["uploads"] == []
    assert manifesto[f"{VIEW}:{CHAVE}"]["etag"] == ETAG_NOVO


def test_head_falhou_cai_pro_get(ambiente):
    sessao = SessaoFake(head=requests.exceptions.ConnectionError("rede caiu"), get=resposta_ok())
    manifesto = {f"{VIEW}:{CHAVE}": entrada(etag=ETAG)}

    assert processar(sessao, manifesto) == "ignorado"
    assert sessao.chamadas == ["HEAD", "GET"]  # o atalho nunca impede a verificação
    assert ambiente["uploads"] == []


def test_head_com_erro_http_cai_pro_get(ambiente):
    sessao = SessaoFake(head=RespostaFake(503), get=resposta_ok())
    manifesto = {f"{VIEW}:{CHAVE}": entrada(etag=ETAG)}

    assert processar(sessao, manifesto) == "ignorado"
    assert sessao.chamadas == ["HEAD", "GET"]


def test_head_404_em_entrada_conhecida_e_indisponivel(ambiente):
    sessao = SessaoFake(head=RespostaFake(404), get=resposta_ok())
    manifesto = {f"{VIEW}:{CHAVE}": entrada(etag=ETAG)}

    assert processar(sessao, manifesto) == "indisponivel"
    assert sessao.chamadas == ["HEAD"]  # mesmo veredito do GET 404, sem baixar
    assert ambiente["uploads"] == []


def test_verificacao_completa_ignora_o_atalho(ambiente, monkeypatch):
    monkeypatch.setattr(ex, "VERIFICACAO_COMPLETA", True)
    sessao = SessaoFake(head=resposta_ok(), get=resposta_ok())
    manifesto = {f"{VIEW}:{CHAVE}": entrada(etag=ETAG)}

    assert processar(sessao, manifesto) == "ignorado"
    assert sessao.chamadas == ["GET"]  # nenhum HEAD: baixou pra conferir o hash


# Manifesto: compatibilidade com o formato anterior (sem etag/last_modified)


def test_manifesto_antigo_sem_colunas_etag_continua_valido(tmp_path):
    caminho = tmp_path / "manifesto.csv"
    colunas_antigas = [c for c in ex.COLUNAS_MANIFESTO if c not in ("etag", "last_modified")]
    linha = {c: v for c, v in entrada().items() if c in colunas_antigas}
    caminho.write_text(
        ",".join(colunas_antigas) + "\n" + ",".join(linha[c] for c in colunas_antigas) + "\n",
        encoding="utf-8",
    )

    manifesto = ex.carregar_manifesto(caminho)
    registro = manifesto[f"{VIEW}:{CHAVE}"]
    assert not registro.get("etag")  # sem ETag conhecido -> cai no GET de sempre

    ex.salvar_manifesto(caminho, manifesto)
    cabecalho, dados = caminho.read_text(encoding="utf-8").splitlines()[:2]
    assert cabecalho.split(",") == ex.COLUNAS_MANIFESTO
    assert dados.endswith(",,")  # colunas novas preenchidas com vazio

    recarregado = ex.carregar_manifesto(caminho)
    assert recarregado[f"{VIEW}:{CHAVE}"]["hash_sha256"] == sha(CONTEUDO)
    assert recarregado[f"{VIEW}:{CHAVE}"]["etag"] == ""


# registrar_entrada: contagem de linhas em streaming (sem copiar o arquivo pra str)


def test_registrar_entrada_conta_linhas_e_colunas_com_campos_multilinha():
    conteudo = b'id,descricao\n1,"linha com\nquebra dentro de aspas"\n2,simples\n'
    manifesto = {}

    ex.registrar_entrada(manifesto, VIEW, CHAVE, URL, conteudo, ETAG, "lm")

    registro = manifesto[f"{VIEW}:{CHAVE}"]
    assert registro["num_linhas"] == 2  # a quebra dentro das aspas não conta
    assert registro["num_colunas"] == 2
    assert registro["tamanho_bytes"] == len(conteudo)
    assert registro["hash_sha256"] == sha(conteudo)
    assert registro["etag"] == ETAG
