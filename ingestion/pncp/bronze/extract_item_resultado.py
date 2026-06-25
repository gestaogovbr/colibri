from datetime import datetime
import duckdb

import utils.ducklake as ducklake


BASE_URL = "https://repositorio.dados.gov.br/seges/comprasgov/anual"

NOME_TABELA = {
    "VW_DM_PNCP_ITEM_RESULTADO": "pncp_item_resultado_bronze",
}


def _tabela_existe(con: duckdb.DuckDBPyConnection, tabela: str) -> bool:
    count = con.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = ?",
        [tabela],
    ).fetchone()[0]
    return count > 0


def _anos_carregados(con: duckdb.DuckDBPyConnection, tabela: str) -> set[int]:
    if not _tabela_existe(con, tabela):
        return set()
    rows = con.execute(f"SELECT DISTINCT ano FROM lake.main.{tabela}").fetchall()
    return {row[0] for row in rows}


def _evoluir_schema(con: duckdb.DuckDBPyConnection, tabela: str):
    lake_cols = {row[0] for row in con.execute(f"DESCRIBE lake.main.{tabela}").fetchall()}
    staging_cols = {row[0] for row in con.execute("DESCRIBE _staging").fetchall()}
    for col in staging_cols - lake_cols:
        con.execute(f'ALTER TABLE lake.main.{tabela} ADD COLUMN "{col}" VARCHAR')
        print(f"[{tabela}] Nova coluna detectada: {col}")


def _inserir_ano(con: duckdb.DuckDBPyConnection, tabela: str, view: str, ano: int):
    url = f"{BASE_URL}/{ano}/comprasGOV-anual-{view}-{ano}.csv"
    print(f"[{tabela}] Inserindo {ano}...")

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _staging AS
        SELECT *, {ano} AS ano
        FROM read_csv_auto('{url}', header=true, all_varchar=true)
    """)

    if not _tabela_existe(con, tabela):
        con.execute(f"CREATE TABLE lake.main.{tabela} AS SELECT * FROM _staging")
    else:
        _evoluir_schema(con, tabela)
        con.execute(f"INSERT INTO lake.main.{tabela} BY NAME SELECT * FROM _staging")

    print(f"[{tabela}] Ano {ano} inserido.")


def _refresh_ano(con: duckdb.DuckDBPyConnection, tabela: str, view: str, ano: int):
    url = f"{BASE_URL}/{ano}/comprasGOV-anual-{view}-{ano}.csv"
    print(f"[{tabela}] Atualizando {ano} (ano corrente)...")

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _staging AS
        SELECT *, {ano} AS ano
        FROM read_csv_auto('{url}', header=true, all_varchar=true)
    """)

    _evoluir_schema(con, tabela)
    con.execute(f"DELETE FROM lake.main.{tabela} WHERE ano = {ano}")
    con.execute(f"INSERT INTO lake.main.{tabela} BY NAME SELECT * FROM _staging")
    print(f"[{tabela}] Ano {ano} atualizado.")


def main(views: list[str], anos: range, caminho_meta: str, data_path: str, nome_segredo: str):
    ano_corrente = datetime.now().year
    con = ducklake.conectar(caminho_meta, data_path, nome_segredo)

    for view in views:
        tabela = NOME_TABELA[view]
        ja_carregados = _anos_carregados(con, tabela)

        for ano in anos:
            if ano in ja_carregados and ano < ano_corrente:
                print(f"[{tabela}] Ano {ano} já carregado. Pulando.")
                continue
            try:
                if ano in ja_carregados and ano == ano_corrente:
                    _refresh_ano(con, tabela, view, ano)
                else:
                    _inserir_ano(con, tabela, view, ano)
            except Exception as e:
                print(f"[{tabela}] Erro ao processar ano {ano}: {e}")

    ducklake.fechar(con, caminho_meta, nome_segredo)
