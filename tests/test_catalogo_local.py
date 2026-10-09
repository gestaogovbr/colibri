"""Os comandos de leitura do lake não podem tocar no meta.ducklake compartilhado.

O meta.ducklake da pasta atual é o arquivo que os pipelines alteram (via dbt) e sobem de volta
ao bucket no fim. Se um `colibri lake query` o sobrescreve no meio do caminho, o pipeline sobe
o catálogo antigo e perde o que acabou de gravar.
"""

import os

from botocore.exceptions import ClientError

from utils.baixar_catalogo import baixar_catalogo
from utils.constantes import CATALOGO_LOCAL


class _ClienteFalso:
    def __init__(self, erro: str | None = None):
        self.erro = erro
        self.destinos: list[str] = []

    def download_file(self, bucket, chave, destino):
        if self.erro:
            raise ClientError({"Error": {"Code": self.erro}}, "GetObject")
        self.destinos.append(destino)
        with open(destino, "w") as f:
            f.write("remoto")


def test_baixar_catalogo_grava_no_destino_informado(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / CATALOGO_LOCAL).write_text("do pipeline")
    destino = tmp_path / "copia" / CATALOGO_LOCAL
    destino.parent.mkdir()

    baixar_catalogo(_ClienteFalso(), "bucket", "meta.ducklake", str(destino))

    assert destino.read_text() == "remoto"
    assert (tmp_path / CATALOGO_LOCAL).read_text() == "do pipeline"


def test_catalogo_inexistente_so_apaga_o_destino(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / CATALOGO_LOCAL).write_text("do pipeline")
    destino = tmp_path / "copia.ducklake"
    destino.write_text("velho")

    baixar_catalogo(_ClienteFalso(erro="404"), "bucket", "meta.ducklake", str(destino))

    assert not destino.exists()
    assert (tmp_path / CATALOGO_LOCAL).read_text() == "do pipeline"


def _capturar_conectar(monkeypatch) -> list[tuple]:
    import utils.ducklake as dl

    chamadas: list[tuple] = []
    monkeypatch.setattr(dl, "conectar", lambda *args: chamadas.append(args))
    return chamadas


def test_conexao_de_leitura_usa_copia_temporaria(tmp_path, monkeypatch):
    from cli import _conectar_lake

    monkeypatch.chdir(tmp_path)
    chamadas = _capturar_conectar(monkeypatch)

    _conectar_lake("colibri-prod", "colibri-token-visualizador")

    (_, _, _, catalogo_local) = chamadas[0]
    assert os.path.basename(catalogo_local) == CATALOGO_LOCAL
    assert os.path.dirname(os.path.abspath(catalogo_local)) != str(tmp_path)


def test_conexao_de_escrita_usa_catalogo_compartilhado(tmp_path, monkeypatch):
    from cli import _conectar_lake

    monkeypatch.chdir(tmp_path)
    chamadas = _capturar_conectar(monkeypatch)

    _conectar_lake("colibri-prod", "colibri-token-desenvolvedor", escrita=True)

    assert chamadas == [("s3://colibri-prod/meta.ducklake", "s3://colibri-prod/lake/", "colibri-token-desenvolvedor")]
