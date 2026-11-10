from pathlib import Path

import duckdb

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
