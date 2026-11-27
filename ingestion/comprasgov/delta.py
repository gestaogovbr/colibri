"""Comparação de snapshots da ingestão ComprasGOV."""

from pathlib import Path

import duckdb


def comparar_snapshots(
    baseline: Path,
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
    chaves_seguras = [f'"{chave}"' for chave in chaves]
    colunas_chave_sql = ", ".join(chaves_seguras)

    caminho_baseline = str(baseline).replace("'", "''")
    caminho_novo = str(novo).replace("'", "''")

    with duckdb.connect(":memory:") as conexao:
        conexao.execute(f"""
            CREATE VIEW anterior AS
            SELECT * FROM read_parquet('{caminho_baseline}')
        """)

        conexao.execute(f"""
            CREATE VIEW atual AS
            SELECT * FROM read_parquet('{caminho_novo}')
        """)

        # //---- 2. Compatibilidade de Schema (Nomes, Tipos e Ordem) ----//

        # Extrai o nome (col[0]) e o tipo (col[1]) preservando a ordem exata da lista
        schema_anterior = [(col[0], col[1]) for col in conexao.execute("DESCRIBE anterior").fetchall()]
        schema_atual = [(col[0], col[1]) for col in conexao.execute("DESCRIBE atual").fetchall()]

        if schema_anterior != schema_atual:
            raise ValueError(
                "Incompatibilidade de schema: Os arquivos possuem colunas ou tipos diferentes, ou em ordem diferente."
            )

        # Verifica se as chaves existem no schema (checando apenas os nomes extraidos)
        nomes_colunas_anterior = [col[0] for col in schema_anterior]
        for chave in chaves:
            if chave not in nomes_colunas_anterior:
                raise ValueError(f"A chave '{chave}' não existe nos arquivos fornecidos.")

        # //---- 3. Validação de Integridade: Chaves Nulas e Duplicadas ----//
        condicao_nulos = " OR ".join([f"{chave_segura} IS NULL" for chave_segura in chaves_seguras])

        for view_name in ["anterior", "atual"]:
            nulos = conexao.execute(f"""
                SELECT COUNT(*) 
                FROM {view_name} 
                WHERE {condicao_nulos}
            """).fetchone()[0]

            if nulos > 0:
                raise ValueError(f"Chaves nulas detectadas na view '{view_name}'. A comparação não é segura.")

            duplicadas = conexao.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT {colunas_chave_sql} 
                    FROM {view_name}
                    GROUP BY {colunas_chave_sql} 
                    HAVING COUNT(*) > 1
                )
            """).fetchone()[0]

            if duplicadas > 0:
                raise ValueError(f"Chaves duplicadas detectadas na view '{view_name}'. A comparação não é segura.")

        # //---- 4. Comparação de Dados (Deltas) ----//
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


def gerar_deltas_parquet(
    baseline: Path, novo: Path, pasta_destino: Path, prefixo_versao: str, chaves: list[str] | str = "cod_compra"
) -> dict:
    # //---- 1. normaliza as chaves e garante o diretorio de destino ----//
    if isinstance(chaves, str):
        chaves = [chaves]

    if not chaves:
        raise ValueError("A lista de chaves primárias não pode estar vazia.")

    chaves_seguras = [f'"{chave}"' for chave in chaves]
    colunas_chave_sql = ", ".join(chaves_seguras)

    caminho_baseline = str(baseline).replace("'", "''")
    caminho_novo = str(novo).replace("'", "''")

    pasta_destino.mkdir(parents=True, exist_ok=True)

    caminho_insert = str(pasta_destino / f"{prefixo_versao}_insert.parquet").replace("'", "''")
    caminho_update = str(pasta_destino / f"{prefixo_versao}_update.parquet").replace("'", "''")
    caminho_delete = str(pasta_destino / f"{prefixo_versao}_delete.parquet").replace("'", "''")

    with duckdb.connect(":memory:") as conexao:
        conexao.execute(f"""
            CREATE VIEW anterior AS
            SELECT * FROM read_parquet('{caminho_baseline}')
        """)

        conexao.execute(f"""
            CREATE VIEW atual AS
            SELECT * FROM read_parquet('{caminho_novo}')
        """)

        # //---- 2. executa o duckdb para gerar o parquet de insercoes com colunas de controle ----//
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

        # //---- 3. executa o duckdb para gerar o parquet de atualizacoes ----//
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

        # //---- 4. executa o duckdb para gerar o parquet de exclusoes ----//
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

        # //---- 5. retorna as contagens lendo diretamente dos arquivos gerados ----//
        inseridos = conexao.execute(f"SELECT COUNT(*) FROM read_parquet('{caminho_insert}')").fetchone()[0]
        atualizados = conexao.execute(f"SELECT COUNT(*) FROM read_parquet('{caminho_update}')").fetchone()[0]
        excluidos = conexao.execute(f"SELECT COUNT(*) FROM read_parquet('{caminho_delete}')").fetchone()[0]

    return {
        "insert": inseridos,
        "update": atualizados,
        "delete": excluidos,
        "arquivos": {"insert": caminho_insert, "update": caminho_update, "delete": caminho_delete},
    }
