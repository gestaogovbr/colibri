import duckdb
import pytest

from ingestion.comprasgov.extract import consultar_metadados, precisa_atualizar
from tests.comprasgov.apoio import FakeResposta, FakeSessao
from utils.manifesto_duckdb import (
    consultar_entrada,
    inicializar_manifesto,
    registrar_entrada,
)


def test_manifesto_registra_e_consulta_etag(tmp_path):
    caminho = tmp_path / "manifesto.duckdb"

    inicializar_manifesto("comprasgov_pncp", caminho)

    registrar_entrada(
        pipeline="comprasgov_pncp",
        tipo="compras",
        ano=2025,
        mes=8,
        etag='"6ae72e66-6f4b29"',
        last_modified="Sun, 01 Nov 2026 12:19:50 GMT",
        hash_sha256="hash_teste",
        caminho=caminho,
    )

    entrada = consultar_entrada(
        "comprasgov_pncp",
        "compras",
        2025,
        8,
        caminho,
    )

    assert entrada is not None
    assert entrada["etag"] == '"6ae72e66-6f4b29"'
    assert entrada["hash_sha256"] == "hash_teste"
    assert entrada["processado_em"] is not None


def test_manifesto_atualiza_etag_sem_duplicar(tmp_path):
    caminho = tmp_path / "manifesto.duckdb"
    inicializar_manifesto("comprasgov_pncp", caminho)

    for etag in ['"versao-1"', '"versao-2"']:
        registrar_entrada(
            pipeline="comprasgov_pncp",
            tipo="compras",
            ano=2025,
            mes=8,
            etag=etag,
            last_modified=None,
            hash_sha256="hash_teste",
            caminho=caminho,
        )

    entrada = consultar_entrada("comprasgov_pncp", "compras", 2025, 8, caminho)

    assert entrada["etag"] == '"versao-2"'

    with duckdb.connect(str(caminho), read_only=True) as conexao:
        quantidade = conexao.execute(
            """
            SELECT COUNT(*)
            FROM manifesto_comprasgov_pncp
            """
        ).fetchone()[0]

    assert quantidade == 1


@pytest.mark.parametrize(
    "etag_remoto, etag_processado, esperado",
    [
        ('"v1"', '"v1"', False),
        ('"v2"', '"v1"', True),
        ('"v1"', None, True),
        (None, '"v1"', True),
    ],
)
def test_comparacao_etags(
    etag_remoto,
    etag_processado,
    esperado,
):
    remoto = {"etag": etag_remoto}
    manifesto = {"etag": etag_processado}

    assert precisa_atualizar(remoto, manifesto) is esperado


def test_arquivo_sem_manifesto_precisa_atualizar():
    remoto = {"etag": '"v1"'}

    assert precisa_atualizar(remoto, None) is True


def test_consultar_metadados_head():
    sessao = FakeSessao(
        lambda url, **kwargs: FakeResposta(
            status_code=200,
            headers={
                "ETag": '"versao-1"',
                "Last-Modified": "Sun, 01 Nov 2026 12:19:50 GMT",
                "Content-Length": "7293737",
            },
        )
    )

    url = "https://exemplo.test/arquivo.zip"

    resultado = consultar_metadados(sessao, url)

    assert resultado == {
        "etag": '"versao-1"',
        "last_modified": "Sun, 01 Nov 2026 12:19:50 GMT",
        "content_length": "7293737",
    }

    assert len(sessao.chamadas) == 1
    assert sessao.urls == [url]
