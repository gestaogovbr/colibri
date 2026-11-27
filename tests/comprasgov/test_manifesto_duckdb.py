import duckdb
import pytest

from ingestion.comprasgov.extract import consultar_metadados, precisa_atualizar
from tests.comprasgov.apoio import FakeResposta, FakeSessao
from utils.manifesto_duckdb import (
    confirmar_aplicacao,
    consultar_aplicacao_pendente,
    consultar_entrada,
    consultar_versoes,
    criar_aplicacao,
    inicializar_manifesto,
    registrar_deltas,
    registrar_entrada,
    registrar_versao,
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


def test_registrar_e_consultar_versao(tmp_path):
    caminho = tmp_path / "manifesto.duckdb"
    inicializar_manifesto("comprasgov_pncp", caminho)

    registrar_versao(
        "comprasgov_pncp",
        "compras",
        2025,
        8,
        "hash_a",
        "versoes/a.parquet",
        '"v1"',
        None,
        caminho=caminho,
    )

    versoes = consultar_versoes("comprasgov_pncp", "compras", 2025, 8, caminho)

    assert len(versoes) == 1
    assert versoes[0]["hash_sha256"] == "hash_a"
    assert versoes[0]["caminho_snapshot"] == "versoes/a.parquet"
    assert versoes[0]["etag"] == '"v1"'
    assert versoes[0]["recebido_em"] is not None
    assert versoes[0]["processado_em"] is None


def test_historico_preserva_multiplas_versoes(tmp_path):
    caminho = tmp_path / "manifesto.duckdb"
    inicializar_manifesto("comprasgov_pncp", caminho)

    for hash_sha256 in ["hash_a", "hash_b", "hash_c"]:
        registrar_versao(
            "comprasgov_pncp",
            "compras",
            2025,
            8,
            hash_sha256,
            f"versoes/{hash_sha256}.parquet",
            None,
            None,
            caminho=caminho,
        )

    versoes = consultar_versoes("comprasgov_pncp", "compras", 2025, 8, caminho)

    assert len(versoes) == 3
    assert {v["hash_sha256"] for v in versoes} == {"hash_a", "hash_b", "hash_c"}


def test_registrar_versao_duplicada_preserva_original(tmp_path):
    caminho = tmp_path / "manifesto.duckdb"
    inicializar_manifesto("comprasgov_pncp", caminho)

    registrar_versao(
        "comprasgov_pncp",
        "compras",
        2025,
        8,
        "hash_a",
        "versoes/a.parquet",
        '"v1"',
        None,
        caminho=caminho,
    )

    antes = consultar_versoes("comprasgov_pncp", "compras", 2025, 8, caminho)

    registrar_versao(
        "comprasgov_pncp",
        "compras",
        2025,
        8,
        "hash_a",
        "versoes/a.parquet",
        '"v2"',
        None,
        caminho=caminho,
    )

    depois = consultar_versoes("comprasgov_pncp", "compras", 2025, 8, caminho)

    assert len(depois) == 1
    assert antes == depois


def test_historico_isola_tipos_e_periodos(tmp_path):
    caminho = tmp_path / "manifesto.duckdb"
    inicializar_manifesto("comprasgov_pncp", caminho)

    for tipo, ano, mes in [
        ("compras", 2025, 8),
        ("compras", 2025, 9),
        ("compras", 2026, 8),
        ("itens", 2025, 8),
    ]:
        registrar_versao(
            "comprasgov_pncp",
            tipo,
            ano,
            mes,
            "hash_a",
            "versoes/a.parquet",
            None,
            None,
            caminho=caminho,
        )

    versoes = consultar_versoes("comprasgov_pncp", "compras", 2025, 8, caminho)

    assert len(versoes) == 1

    assert consultar_versoes("comprasgov_pncp", "resultados", 2025, 8, caminho) == []


def test_registrar_versao_nao_altera_baseline(tmp_path):
    caminho = tmp_path / "manifesto.duckdb"
    inicializar_manifesto("comprasgov_pncp", caminho)

    registrar_entrada(
        "comprasgov_pncp",
        "compras",
        2025,
        8,
        '"v1"',
        None,
        "hash_a",
        caminho=caminho,
        caminho_snapshot="versoes/a.parquet",
    )

    antes = consultar_entrada("comprasgov_pncp", "compras", 2025, 8, caminho)

    registrar_versao(
        "comprasgov_pncp",
        "compras",
        2025,
        8,
        "hash_b",
        "versoes/b.parquet",
        '"v2"',
        None,
        caminho=caminho,
    )

    depois = consultar_entrada("comprasgov_pncp", "compras", 2025, 8, caminho)

    assert antes == depois
    assert depois["hash_sha256"] == "hash_a"


def test_ciclo_aplicacao(tmp_path):
    caminho = tmp_path / "manifesto.duckdb"
    pipeline = "comprasgov_pncp"

    inicializar_manifesto(pipeline, caminho)

    assert consultar_aplicacao_pendente(pipeline, "compras", 2025, 8, caminho) is None

    id_aplicacao = criar_aplicacao(
        pipeline,
        "compras",
        2025,
        8,
        None,
        "hash_a",
        caminho,
    )

    aplicacao = consultar_aplicacao_pendente(pipeline, "compras", 2025, 8, caminho)

    assert aplicacao["id_aplicacao"] == id_aplicacao
    assert aplicacao["status"] == "pendente"
    assert aplicacao["hash_anterior"] is None
    assert aplicacao["hash_novo"] == "hash_a"

    registrar_deltas(
        pipeline,
        id_aplicacao,
        "deltas/aplicacao_000001",
        10,
        2,
        1,
        caminho,
    )

    aplicacao = consultar_aplicacao_pendente(pipeline, "compras", 2025, 8, caminho)

    assert aplicacao["status"] == "deltas_gerados"
    assert aplicacao["qtd_insert"] == 10
    assert aplicacao["qtd_update"] == 2
    assert aplicacao["qtd_delete"] == 1

    assert consultar_entrada(pipeline, "compras", 2025, 8, caminho) is None


def test_confirmar_primeira_aplicacao(tmp_path):
    caminho = tmp_path / "manifesto.duckdb"
    pipeline = "comprasgov_pncp"

    inicializar_manifesto(pipeline, caminho)

    registrar_versao(
        pipeline,
        "compras",
        2025,
        8,
        "hash_a",
        "versoes/a.parquet",
        '"v1"',
        None,
        caminho=caminho,
    )

    id_aplicacao = criar_aplicacao(
        pipeline,
        "compras",
        2025,
        8,
        None,
        "hash_a",
        caminho,
    )

    registrar_deltas(
        pipeline,
        id_aplicacao,
        "deltas/aplicacao_000001",
        10,
        0,
        0,
        caminho,
    )

    confirmar_aplicacao(pipeline, id_aplicacao, caminho)

    entrada = consultar_entrada(pipeline, "compras", 2025, 8, caminho)

    assert entrada["hash_sha256"] == "hash_a"
    assert entrada["caminho_snapshot"] == "versoes/a.parquet"
    assert entrada["etag"] == '"v1"'
    assert entrada["processado_em"] is not None

    assert consultar_aplicacao_pendente(pipeline, "compras", 2025, 8, caminho) is None

    versoes = consultar_versoes(pipeline, "compras", 2025, 8, caminho)

    assert versoes[0]["processado_em"] is not None

    with duckdb.connect(str(caminho), read_only=True) as conexao:
        status, processado_em = conexao.execute(
            """
            SELECT status, processado_em
            FROM aplicacoes_comprasgov_pncp
            WHERE id_aplicacao = ?
        """,
            [id_aplicacao],
        ).fetchone()

    assert status == "processado"
    assert processado_em is not None


def test_confirmar_multiplas_aplicacoes(tmp_path):
    caminho = tmp_path / "manifesto.duckdb"
    pipeline = "comprasgov_pncp"

    inicializar_manifesto(pipeline, caminho)

    for hash_novo in ["hash_a", "hash_b"]:
        registrar_versao(
            pipeline,
            "compras",
            2025,
            8,
            hash_novo,
            f"versoes/{hash_novo}.parquet",
            None,
            None,
            caminho=caminho,
        )

    hash_anterior = None
    ids = []

    for hash_novo in ["hash_a", "hash_b", "hash_a"]:
        id_aplicacao = criar_aplicacao(
            pipeline,
            "compras",
            2025,
            8,
            hash_anterior,
            hash_novo,
            caminho,
        )

        ids.append(id_aplicacao)

        registrar_deltas(
            pipeline,
            id_aplicacao,
            f"deltas/aplicacao_{id_aplicacao:06d}",
            1,
            0,
            0,
            caminho,
        )

        confirmar_aplicacao(pipeline, id_aplicacao, caminho)

        entrada = consultar_entrada(pipeline, "compras", 2025, 8, caminho)

        assert entrada["hash_sha256"] == hash_novo

        hash_anterior = hash_novo

    assert len(set(ids)) == 3

    assert len(consultar_versoes(pipeline, "compras", 2025, 8, caminho)) == 2

    with duckdb.connect(str(caminho), read_only=True) as conexao:
        quantidade = conexao.execute("""
            SELECT COUNT(*)
            FROM aplicacoes_comprasgov_pncp
            WHERE status = 'processado'
        """).fetchone()[0]

    assert quantidade == 3


def test_confirmar_aplicacao_sem_deltas(tmp_path):
    caminho = tmp_path / "manifesto.duckdb"
    pipeline = "comprasgov_pncp"

    inicializar_manifesto(pipeline, caminho)

    id_aplicacao = criar_aplicacao(
        pipeline,
        "compras",
        2025,
        8,
        None,
        "hash_a",
        caminho,
    )

    with pytest.raises(ValueError, match="não pode ser confirmada"):
        confirmar_aplicacao(pipeline, id_aplicacao, caminho)

    aplicacao = consultar_aplicacao_pendente(pipeline, "compras", 2025, 8, caminho)

    assert aplicacao["status"] == "pendente"

    assert consultar_entrada(pipeline, "compras", 2025, 8, caminho) is None


def test_confirmar_aplicacao_baseline_divergente(tmp_path):
    caminho = tmp_path / "manifesto.duckdb"
    pipeline = "comprasgov_pncp"

    inicializar_manifesto(pipeline, caminho)

    registrar_entrada(
        pipeline,
        "compras",
        2025,
        8,
        None,
        None,
        "hash_a",
        caminho=caminho,
    )

    registrar_versao(
        pipeline,
        "compras",
        2025,
        8,
        "hash_c",
        "versoes/c.parquet",
        None,
        None,
        caminho=caminho,
    )

    id_aplicacao = criar_aplicacao(
        pipeline,
        "compras",
        2025,
        8,
        "hash_b",
        "hash_c",
        caminho,
    )

    registrar_deltas(
        pipeline,
        id_aplicacao,
        "deltas/aplicacao_000001",
        1,
        0,
        0,
        caminho,
    )

    antes = consultar_entrada(pipeline, "compras", 2025, 8, caminho)

    with pytest.raises(ValueError, match="baseline atual diverge"):
        confirmar_aplicacao(pipeline, id_aplicacao, caminho)

    depois = consultar_entrada(pipeline, "compras", 2025, 8, caminho)

    assert antes == depois

    assert consultar_aplicacao_pendente(pipeline, "compras", 2025, 8, caminho)["status"] == "deltas_gerados"

    assert consultar_versoes(pipeline, "compras", 2025, 8, caminho)[0]["processado_em"] is None
