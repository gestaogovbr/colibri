import io
import zipfile

import duckdb
import pytest

from ingestion.comprasgov import extract, pipeline
from tests.comprasgov.apoio import CSV_PADRAO, FakeSessao
from utils.manifesto_duckdb import (
    confirmar_aplicacao,
    consultar_aplicacao_pendente,
    consultar_entrada,
)


def criar_sessao(csv: bytes, etag: str):
    """Simula um ZIP do DELOG com ETag controlada."""
    buffer = io.BytesIO()

    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as arquivo:
        arquivo.writestr("dados.csv", csv)

    return FakeSessao.com_zip(buffer.getvalue(), etag=etag)


def configurar_teste(tmp_path, monkeypatch):
    """Isola snapshots e manifesto do ambiente real."""
    monkeypatch.chdir(tmp_path)

    destino = tmp_path / "snapshots"
    monkeypatch.setattr(extract, "DIRETORIO_SAIDA", destino)
    monkeypatch.setattr(pipeline, "DIRETORIO_SAIDA", destino)


def csv_alterado():
    """Simula uma nova versão com INSERT, UPDATE e DELETE."""
    linhas = CSV_PADRAO.decode("utf-8").splitlines()

    linhas = [
        linha.replace("Aquisição de café", "Aquisição de chá") for linha in linhas if not linha.startswith("002;")
    ]

    linhas.append("004;Nova compra;12345678000100;5,00")

    return ("\n".join(linhas) + "\n").encode("utf-8")


def test_primeira_ingestao_prepara_aplicacao(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    destino = tmp_path / "snapshots"
    monkeypatch.setattr(extract, "DIRETORIO_SAIDA", destino)
    monkeypatch.setattr(pipeline, "DIRETORIO_SAIDA", destino)

    sessao = FakeSessao.com_csv(CSV_PADRAO)

    resultado = pipeline.preparar_aplicacao(sessao, 2025, 8, "compras")

    assert resultado["status"] == "preparado"
    assert resultado["insert"] == 3
    assert resultado["update"] == 0
    assert resultado["delete"] == 0

    aplicacao = consultar_aplicacao_pendente("comprasgov_pncp", "compras", 2025, 8)

    assert aplicacao is not None
    assert aplicacao["status"] == "deltas_gerados"
    assert aplicacao["id_aplicacao"] == resultado["id_aplicacao"]

    # Preparar não significa confirmar a baseline.
    assert consultar_entrada("comprasgov_pncp", "compras", 2025, 8) is None

    caminho = resultado["arquivos"]["insert"]

    with duckdb.connect(":memory:") as conexao:
        total = conexao.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [caminho],
        ).fetchone()[0]

    assert total == 3


def test_reexecucao_reutiliza_aplicacao(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    destino = tmp_path / "snapshots"
    monkeypatch.setattr(extract, "DIRETORIO_SAIDA", destino)
    monkeypatch.setattr(pipeline, "DIRETORIO_SAIDA", destino)

    sessao = FakeSessao.com_csv(CSV_PADRAO)

    primeira = pipeline.preparar_aplicacao(sessao, 2025, 8, "compras")

    assert primeira["status"] == "preparado"

    # Na segunda execução, estas funções não podem ser chamadas.
    def falha_inesperada(*args, **kwargs):
        raise AssertionError("Reexecução não deveria extrair ou gerar deltas")

    monkeypatch.setattr(
        pipeline,
        "processar_tipo_detalhado",
        falha_inesperada,
    )
    monkeypatch.setattr(
        pipeline,
        "gerar_deltas_parquet",
        falha_inesperada,
    )

    segunda = pipeline.preparar_aplicacao(sessao, 2025, 8, "compras")

    assert segunda["status"] == "preparado"
    assert segunda["id_aplicacao"] == primeira["id_aplicacao"]
    assert segunda["caminho_deltas"] == primeira["caminho_deltas"]

    aplicacao = consultar_aplicacao_pendente("comprasgov_pncp", "compras", 2025, 8)

    assert aplicacao["status"] == "deltas_gerados"

    assert consultar_entrada("comprasgov_pncp", "compras", 2025, 8) is None


def test_atualizacao_gera_deltas_corretos(tmp_path, monkeypatch):
    configurar_teste(tmp_path, monkeypatch)

    primeira = pipeline.preparar_aplicacao(
        criar_sessao(CSV_PADRAO, "v1"),
        2025,
        8,
        "compras",
    )

    confirmar_aplicacao(
        "comprasgov_pncp",
        primeira["id_aplicacao"],
    )

    baseline_anterior = consultar_entrada("comprasgov_pncp", "compras", 2025, 8)

    segunda = pipeline.preparar_aplicacao(
        criar_sessao(csv_alterado(), "v2"),
        2025,
        8,
        "compras",
    )

    assert segunda["status"] == "preparado"
    assert segunda["insert"] == 1
    assert segunda["update"] == 1
    assert segunda["delete"] == 1

    assert segunda["id_aplicacao"] != primeira["id_aplicacao"]

    # A nova aplicação ainda não foi confirmada.
    baseline_atual = consultar_entrada("comprasgov_pncp", "compras", 2025, 8)

    assert baseline_atual["hash_sha256"] == baseline_anterior["hash_sha256"]


def test_sem_mudancas_nao_cria_aplicacao(tmp_path, monkeypatch):
    configurar_teste(tmp_path, monkeypatch)

    primeira = pipeline.preparar_aplicacao(
        criar_sessao(CSV_PADRAO, "v1"),
        2025,
        8,
        "compras",
    )

    confirmar_aplicacao(
        "comprasgov_pncp",
        primeira["id_aplicacao"],
    )

    segunda = pipeline.preparar_aplicacao(
        criar_sessao(CSV_PADRAO, "v1"),
        2025,
        8,
        "compras",
    )

    assert segunda["status"] == "existente"

    assert consultar_aplicacao_pendente("comprasgov_pncp", "compras", 2025, 8) is None


def test_falha_na_geracao_permite_retomada(tmp_path, monkeypatch):
    configurar_teste(tmp_path, monkeypatch)

    def falhar(*args, **kwargs):
        raise RuntimeError("Falha simulada nos deltas")

    with monkeypatch.context() as m:
        m.setattr(pipeline, "gerar_deltas_parquet", falhar)

        with pytest.raises(RuntimeError, match="Falha simulada"):
            pipeline.preparar_aplicacao(
                criar_sessao(CSV_PADRAO, "v1"),
                2025,
                8,
                "compras",
            )

    pendente = consultar_aplicacao_pendente("comprasgov_pncp", "compras", 2025, 8)

    assert pendente is not None
    assert pendente["status"] == "pendente"

    assert consultar_entrada("comprasgov_pncp", "compras", 2025, 8) is None

    # A falha simulada foi removida.
    recuperada = pipeline.preparar_aplicacao(
        criar_sessao(CSV_PADRAO, "v1"),
        2025,
        8,
        "compras",
    )

    assert recuperada["status"] == "preparado"
    assert recuperada["insert"] == 3
    assert recuperada["id_aplicacao"] == pendente["id_aplicacao"]


def test_interrupcao_apos_publicar_deltas(tmp_path, monkeypatch):
    configurar_teste(tmp_path, monkeypatch)

    def falhar_registro(*args, **kwargs):
        raise RuntimeError("Falha ao registrar deltas")

    with monkeypatch.context() as m:
        m.setattr(
            pipeline,
            "registrar_deltas",
            falhar_registro,
        )

        with pytest.raises(RuntimeError, match="Falha ao registrar"):
            pipeline.preparar_aplicacao(
                criar_sessao(CSV_PADRAO, "v1"),
                2025,
                8,
                "compras",
            )

    pendente = consultar_aplicacao_pendente("comprasgov_pncp", "compras", 2025, 8)

    assert pendente["status"] == "pendente"

    destino = tmp_path / "snapshots/compras/2025/08/deltas" / f"aplicacao_{pendente['id_aplicacao']:06d}"

    assert (destino / "insert.parquet").exists()
    assert (destino / "update.parquet").exists()
    assert (destino / "delete.parquet").exists()

    # Comportamento atual: bloqueia a retomada insegura.
    with pytest.raises(
        RuntimeError,
        match="Deltas já publicados sem registro",
    ):
        pipeline.preparar_aplicacao(
            criar_sessao(CSV_PADRAO, "v1"),
            2025,
            8,
            "compras",
        )

    assert consultar_entrada("comprasgov_pncp", "compras", 2025, 8) is None


def test_retorno_a_versao_anterior(tmp_path, monkeypatch):
    configurar_teste(tmp_path, monkeypatch)

    # Versão A
    primeira = pipeline.preparar_aplicacao(
        criar_sessao(CSV_PADRAO, "v1"),
        2025,
        8,
        "compras",
    )

    confirmar_aplicacao(
        "comprasgov_pncp",
        primeira["id_aplicacao"],
    )

    # Versão B
    segunda = pipeline.preparar_aplicacao(
        criar_sessao(csv_alterado(), "v2"),
        2025,
        8,
        "compras",
    )

    confirmar_aplicacao(
        "comprasgov_pncp",
        segunda["id_aplicacao"],
    )

    # A fonte retorna ao conteúdo da versão A.
    terceira = pipeline.preparar_aplicacao(
        criar_sessao(CSV_PADRAO, "v3"),
        2025,
        8,
        "compras",
    )

    assert terceira["status"] == "preparado"
    assert terceira["id_aplicacao"] != primeira["id_aplicacao"]
    assert terceira["id_aplicacao"] != segunda["id_aplicacao"]

    assert terceira["insert"] == 1
    assert terceira["update"] == 1
    assert terceira["delete"] == 1
