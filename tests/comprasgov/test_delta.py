from pathlib import Path

import duckdb
import pytest

from ingestion.comprasgov.delta import comparar_snapshots, gerar_deltas_parquet


def test_comparar_e_gerar_deltas(tmp_path: Path):
    # //---- prepara os caminhos efemeros usando a fixture tmp_path do pytest no linux ----//
    caminho_baseline = tmp_path / "baseline.parquet"
    caminho_novo = tmp_path / "novo.parquet"
    pasta_destino = tmp_path / "deltas"

    # //---- injeta dados controlados simulando os arquivos da seges ----//
    with duckdb.connect(":memory:") as conexao:
        conexao.execute(f"""
            COPY (
                SELECT *
                FROM (
                    VALUES
                        ('001', 'Computadores', '1000'),
                        ('002', 'Cadeiras', '500'),
                        ('003', 'Mesas', '800')
                ) AS t(cod_compra, descricao, valor)
            ) TO '{caminho_baseline}' (FORMAT PARQUET)
        """)

        conexao.execute(f"""
            COPY (
                SELECT *
                FROM (
                    VALUES
                        ('001', 'Computadores', '1200'), 
                        ('003', 'Mesas', '800'),         
                        ('004', 'Monitores', '900')      
                ) AS t(cod_compra, descricao, valor)
            ) TO '{caminho_novo}' (FORMAT PARQUET)
        """)

    # //---- testa a validacao estrutural e a integridade matematica dos joints ----//
    resultado_comparacao = comparar_snapshots(baseline=caminho_baseline, novo=caminho_novo, chaves="cod_compra")

    assert resultado_comparacao["insert"] == 1
    assert resultado_comparacao["update"] == 1
    assert resultado_comparacao["delete"] == 1

    # //---- testa a exportacao dos arquivos fisicos de delta com colunas de scd2 ----//
    resultado_deltas = gerar_deltas_parquet(
        baseline=caminho_baseline,
        novo=caminho_novo,
        pasta_destino=pasta_destino,
        prefixo_versao="compras_2026_01_V002",
        chaves="cod_compra",
    )

    # //---- garante que a leitura dos arquivos recem criados mantem a contagem correta ----//
    assert resultado_deltas["insert"] == 1
    assert resultado_deltas["update"] == 1
    assert resultado_deltas["delete"] == 1

    caminho_insert = Path(resultado_deltas["arquivos"]["insert"])
    caminho_update = Path(resultado_deltas["arquivos"]["update"])
    caminho_delete = Path(resultado_deltas["arquivos"]["delete"])

    # //---- inspeciona o sistema de arquivos para confirmar a persistencia de disco ----//
    assert caminho_insert.exists()
    assert caminho_update.exists()
    assert caminho_delete.exists()

    # //---- interroga os arquivos finais para validar a injecao dinamica do ddl ----//
    with duckdb.connect(":memory:") as conexao:
        operacao_update = conexao.execute(f"SELECT _operacao FROM read_parquet('{caminho_update}')").fetchone()[0]
        assert operacao_update == "UPDATE"


def test_gerar_deltas_com_chave_composta(tmp_path):
    import duckdb

    from ingestion.comprasgov.delta import gerar_deltas_parquet

    baseline = tmp_path / "baseline.parquet"
    novo = tmp_path / "novo.parquet"
    destino = tmp_path / "deltas"

    with duckdb.connect(":memory:") as conexao:
        conexao.execute(f"""
            COPY (
                SELECT * FROM (
                    VALUES
                        ('100', '501', '1000'),
                        ('100', '502', '900'),
                        ('200', '503', '500')
                ) AS t(srk_pncp_item_compra, COD_RESULTADO_ITEM, valor)
            ) TO '{baseline}' (FORMAT PARQUET)
        """)

        conexao.execute(f"""
            COPY (
                SELECT * FROM (
                    VALUES
                        ('100', '501', '1200'),
                        ('100', '504', '800'),
                        ('200', '503', '500')
                ) AS t(srk_pncp_item_compra, COD_RESULTADO_ITEM, valor)
            ) TO '{novo}' (FORMAT PARQUET)
        """)

    resultado = gerar_deltas_parquet(
        baseline=baseline,
        novo=novo,
        pasta_destino=destino,
        prefixo_versao="teste",
        chaves=["srk_pncp_item_compra", "COD_RESULTADO_ITEM"],
    )

    assert resultado["insert"] == 1
    assert resultado["update"] == 1
    assert resultado["delete"] == 1

    with duckdb.connect(":memory:") as conexao:
        inseridos = conexao.execute(
            "SELECT COD_RESULTADO_ITEM FROM read_parquet(?)",
            [resultado["arquivos"]["insert"]],
        ).fetchall()

        atualizados = conexao.execute(
            "SELECT COD_RESULTADO_ITEM, valor FROM read_parquet(?)",
            [resultado["arquivos"]["update"]],
        ).fetchall()

        excluidos = conexao.execute(
            "SELECT COD_RESULTADO_ITEM FROM read_parquet(?)",
            [resultado["arquivos"]["delete"]],
        ).fetchall()

    assert inseridos == [("504",)]
    assert atualizados == [("501", "1200")]
    assert excluidos == [("502",)]


def test_gerar_deltas_rejeita_chaves_duplicadas(tmp_path):
    baseline = tmp_path / "baseline.parquet"
    novo = tmp_path / "novo.parquet"
    destino = tmp_path / "deltas"

    with duckdb.connect(":memory:") as conexao:
        conexao.execute(f"""
            COPY (
                SELECT * FROM (
                    VALUES
                        ('100', '501', '1000'),
                        ('100', '502', '900')
                ) AS t(srk, resultado, valor)
            ) TO '{baseline}' (FORMAT PARQUET)
        """)

        conexao.execute(f"""
            COPY (
                SELECT * FROM (
                    VALUES
                        ('100', '501', '1200'),
                        ('100', '501', '1300')
                ) AS t(srk, resultado, valor)
            ) TO '{novo}' (FORMAT PARQUET)
        """)

    with pytest.raises(ValueError, match="duplicadas"):
        gerar_deltas_parquet(
            baseline,
            novo,
            destino,
            "teste",
            chaves=["srk", "resultado"],
        )

    assert not destino.exists()


def test_gerar_deltas_rejeita_chaves_nulas(tmp_path):
    baseline = tmp_path / "baseline.parquet"
    novo = tmp_path / "novo.parquet"
    destino = tmp_path / "deltas"

    with duckdb.connect(":memory:") as conexao:
        conexao.execute(f"""
            COPY (
                SELECT * FROM (
                    VALUES
                        ('100', '501', '1000')
                ) AS t(srk, resultado, valor)
            ) TO '{baseline}' (FORMAT PARQUET)
        """)

        conexao.execute(f"""
            COPY (
                SELECT * FROM (
                    VALUES
                        ('100', NULL::VARCHAR, '1200')
                ) AS t(srk, resultado, valor)
            ) TO '{novo}' (FORMAT PARQUET)
        """)

    with pytest.raises(ValueError, match="nulas"):
        gerar_deltas_parquet(
            baseline,
            novo,
            destino,
            "teste",
            chaves=["srk", "resultado"],
        )

    assert not destino.exists()


def test_primeira_ingestao_sem_baseline(tmp_path):
    novo = tmp_path / "novo.parquet"
    destino = tmp_path / "deltas"

    duckdb.sql("""
        SELECT * FROM (
            VALUES
                ('001', 'Computadores'),
                ('002', 'Cadeiras'),
                ('003', 'Mesas')
        ) AS t(cod_compra, descricao)
    """).write_parquet(str(novo))

    comparacao = comparar_snapshots(None, novo)

    assert comparacao == {
        "insert": 3,
        "update": 0,
        "delete": 0,
    }

    resultado = gerar_deltas_parquet(
        baseline=None,
        novo=novo,
        pasta_destino=destino,
        prefixo_versao="teste",
    )

    assert resultado["insert"] == 3
    assert resultado["update"] == 0
    assert resultado["delete"] == 0

    with duckdb.connect(":memory:") as conexao:
        operacoes = conexao.execute(
            "SELECT DISTINCT _operacao FROM read_parquet(?)",
            [resultado["arquivos"]["insert"]],
        ).fetchall()

    assert operacoes == [("INSERT",)]


def test_snapshots_identicos_nao_geram_alteracoes(tmp_path):
    snapshot = tmp_path / "snapshot.parquet"

    duckdb.sql("""
        SELECT '001' AS cod_compra, 'Computadores' AS descricao
    """).write_parquet(str(snapshot))

    assert comparar_snapshots(snapshot, snapshot) == {
        "insert": 0,
        "update": 0,
        "delete": 0,
    }

    resultado = gerar_deltas_parquet(
        snapshot,
        snapshot,
        tmp_path / "deltas",
        "teste",
    )

    assert resultado["insert"] == 0
    assert resultado["update"] == 0
    assert resultado["delete"] == 0


@pytest.mark.parametrize(
    "consulta, mensagem",
    [
        (
            "SELECT NULL::VARCHAR AS cod_compra",
            "nulas",
        ),
        (
            "SELECT '001' AS cod_compra FROM range(2)",
            "duplicadas",
        ),
    ],
)
def test_primeira_ingestao_rejeita_chaves_invalidas(tmp_path, consulta, mensagem):
    novo = tmp_path / "novo.parquet"
    destino = tmp_path / "deltas"

    duckdb.sql(consulta).write_parquet(str(novo))

    with pytest.raises(ValueError, match=mensagem):
        gerar_deltas_parquet(None, novo, destino, "teste")

    assert not destino.exists()


def test_deltas_nao_sobrescrevem_destino(tmp_path):
    novo = tmp_path / "novo.parquet"
    destino = tmp_path / "deltas"

    duckdb.sql("SELECT '001' AS cod_compra").write_parquet(str(novo))

    gerar_deltas_parquet(None, novo, destino, "teste")

    antes = {arquivo.name: arquivo.read_bytes() for arquivo in destino.glob("*.parquet")}

    with pytest.raises(FileExistsError):
        gerar_deltas_parquet(None, novo, destino, "teste")

    depois = {arquivo.name: arquivo.read_bytes() for arquivo in destino.glob("*.parquet")}

    assert antes == depois
    assert len(depois) == 3


def test_falha_na_geracao_nao_publica_deltas(tmp_path, monkeypatch):
    import ingestion.comprasgov.delta as delta

    destino = tmp_path / "deltas"

    def simular_falha(baseline, novo, pasta_destino, chaves):
        pasta_destino.mkdir()
        (pasta_destino / "insert.parquet").touch()

        raise RuntimeError("Falha simulada no UPDATE")

    monkeypatch.setattr(
        delta,
        "_gerar_deltas_parquet",
        simular_falha,
    )

    with pytest.raises(RuntimeError, match="Falha simulada"):
        gerar_deltas_parquet(
            None,
            tmp_path / "novo.parquet",
            destino,
            "teste",
        )

    assert not destino.exists()
    assert not list(tmp_path.glob(".deltas_*"))
