"""Comparação de snapshots da ingestão ComprasGOV."""

import os
import tempfile
from pathlib import Path

import duckdb


def _criar_views(conexao, baseline: Path | None, novo: Path):
    """Cria as views utilizadas na comparação dos snapshots."""

    caminho_novo = str(novo).replace("'", "''")

    conexao.execute(f"""
        CREATE VIEW atual AS
        SELECT * FROM read_parquet('{caminho_novo}')
    """)

    if baseline is None:
        conexao.execute("""
            CREATE VIEW anterior AS
            SELECT * FROM atual WHERE FALSE
        """)
    else:
        caminho_baseline = str(baseline).replace("'", "''")

        conexao.execute(f"""
            CREATE VIEW anterior AS
            SELECT * FROM read_parquet('{caminho_baseline}')
        """)


def _validar_snapshots(conexao, chaves):
    """Valida schemas e integridade das chaves dos snapshots."""

    schema_anterior = [(col[0], col[1]) for col in conexao.execute("DESCRIBE anterior").fetchall()]

    schema_atual = [(col[0], col[1]) for col in conexao.execute("DESCRIBE atual").fetchall()]

    if schema_anterior != schema_atual:
        raise ValueError("Incompatibilidade de schema entre os snapshots.")

    nomes_colunas = [col[0] for col in schema_anterior]

    for chave in chaves:
        if chave not in nomes_colunas:
            raise ValueError(f"A chave '{chave}' não existe nos snapshots.")

    chaves_seguras = [f'"{chave.replace(chr(34), chr(34) * 2)}"' for chave in chaves]
    colunas_chave_sql = ", ".join(chaves_seguras)

    condicao_nulos = " OR ".join(f"{chave} IS NULL" for chave in chaves_seguras)

    for view_name in ["anterior", "atual"]:
        nulos = conexao.execute(f"""
            SELECT COUNT(*)
            FROM {view_name}
            WHERE {condicao_nulos}
        """).fetchone()[0]

        if nulos:
            raise ValueError(f"Chaves nulas detectadas na view '{view_name}'.")

        duplicadas = conexao.execute(f"""
            SELECT COUNT(*)
            FROM (
                SELECT {colunas_chave_sql}
                FROM {view_name}
                GROUP BY {colunas_chave_sql}
                HAVING COUNT(*) > 1
            )
        """).fetchone()[0]

        if duplicadas:
            raise ValueError(f"Chaves duplicadas detectadas na view '{view_name}'.")


def comparar_snapshots(
    baseline: Path | None,
    novo: Path,
    chaves: list[str] | str = "cod_compra",
) -> dict:
    """Conta inserções, atualizações e exclusões entre dois Parquets."""

    # //---- 1. Normaliza e Valida a Chave Primária ----//
    if isinstance(chaves, str):
        chaves = [chaves]

    if not chaves:
        raise ValueError("A lista de chaves primárias não pode estar vazia.")

    # //---- Delimita os identificadores SQL com aspas duplas para evitar erros de sintaxe ----//
    chaves_seguras = [f'"{chave.replace(chr(34), chr(34) * 2)}"' for chave in chaves]
    colunas_chave_sql = ", ".join(chaves_seguras)

    with duckdb.connect(":memory:") as conexao:
        _criar_views(conexao, baseline, novo)

        # //---- 2. Validar os snapshots ----//
        _validar_snapshots(conexao, chaves)

        # //---- 3. Comparação de Dados (Deltas) ----//
        inseridos = conexao.execute(f"""
            SELECT COUNT(*)
            FROM atual AS n
            LEFT JOIN anterior AS a USING ({colunas_chave_sql})
            WHERE a.{chaves_seguras[0]} IS NULL
        """).fetchone()[0]

        excluidos = conexao.execute(f"""
            SELECT COUNT(*)
            FROM anterior AS a
            LEFT JOIN atual AS n USING ({colunas_chave_sql})
            WHERE n.{chaves_seguras[0]} IS NULL
        """).fetchone()[0]

        atualizados = conexao.execute(f"""
            SELECT COUNT(*)
            FROM atual AS n
            INNER JOIN anterior AS a USING ({colunas_chave_sql})
            WHERE n IS DISTINCT FROM a
        """).fetchone()[0]

    return {
        "insert": inseridos,
        "update": atualizados,
        "delete": excluidos,
    }


def _gerar_deltas_parquet(
    baseline: Path | None,
    novo: Path,
    pasta_destino: Path,
    chaves: list[str] | str = "cod_compra",
) -> dict:
    """Gera os arquivos Parquet de inserções, atualizações e exclusões."""

    if isinstance(chaves, str):
        chaves = [chaves]

    if not chaves:
        raise ValueError("A lista de chaves primárias não pode estar vazia.")

    chaves_seguras = [f'"{chave.replace(chr(34), chr(34) * 2)}"' for chave in chaves]
    colunas_chave_sql = ", ".join(chaves_seguras)

    caminho_insert = str(pasta_destino / "insert.parquet").replace("'", "''")
    caminho_update = str(pasta_destino / "update.parquet").replace("'", "''")
    caminho_delete = str(pasta_destino / "delete.parquet").replace("'", "''")

    with duckdb.connect(":memory:") as conexao:
        _criar_views(conexao, baseline, novo)
        _validar_snapshots(conexao, chaves)

        pasta_destino.mkdir(parents=True, exist_ok=True)

        # //---- 1. Gerar inserções ----//
        conexao.execute(f"""
            COPY (
                SELECT n.*,
                    'INSERT' AS _operacao,
                    CURRENT_TIMESTAMP AS _processado_em
                FROM atual AS n
                LEFT JOIN anterior AS a USING ({colunas_chave_sql})
                WHERE a.{chaves_seguras[0]} IS NULL
            ) TO '{caminho_insert}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        # //---- 2. Gerar atualizações ----//
        conexao.execute(f"""
            COPY (
                SELECT n.*,
                    'UPDATE' AS _operacao,
                    CURRENT_TIMESTAMP AS _processado_em
                FROM atual AS n
                INNER JOIN anterior AS a USING ({colunas_chave_sql})
                WHERE n IS DISTINCT FROM a
            ) TO '{caminho_update}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        # //---- 3. Gerar exclusões ----//
        conexao.execute(f"""
            COPY (
                SELECT a.*,
                    'DELETE' AS _operacao,
                    CURRENT_TIMESTAMP AS _processado_em
                FROM anterior AS a
                LEFT JOIN atual AS n USING ({colunas_chave_sql})
                WHERE n.{chaves_seguras[0]} IS NULL
            ) TO '{caminho_delete}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        # //---- 4. Contabilizar os registros ----//
        inseridos = conexao.execute(f"SELECT COUNT(*) FROM read_parquet('{caminho_insert}')").fetchone()[0]

        atualizados = conexao.execute(f"SELECT COUNT(*) FROM read_parquet('{caminho_update}')").fetchone()[0]

        excluidos = conexao.execute(f"SELECT COUNT(*) FROM read_parquet('{caminho_delete}')").fetchone()[0]

    return {
        "insert": inseridos,
        "update": atualizados,
        "delete": excluidos,
    }


def gerar_deltas_parquet(
    baseline: Path | None,
    novo: Path,
    pasta_destino: Path,
    prefixo_versao: str,
    chaves: list[str] | str = "cod_compra",
) -> dict:
    """Gera e publica um conjunto completo de deltas."""

    if pasta_destino.exists():
        raise FileExistsError(f"Destino de deltas já existente: {pasta_destino}")

    pasta_destino.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(
        prefix=".deltas_",
        dir=pasta_destino.parent,
    ) as temporario:
        pasta_temporaria = Path(temporario) / "resultado"

        resultado = _gerar_deltas_parquet(
            baseline,
            novo,
            pasta_temporaria,
            chaves,
        )

        os.rename(pasta_temporaria, pasta_destino)

    resultado["arquivos"] = {
        operacao: str(pasta_destino / f"{operacao}.parquet") for operacao in ("insert", "update", "delete")
    }

    return resultado
