import duckdb

import shared.ducklake as ducklake


BASE_URL = "https://repositorio.dados.gov.br/seges/comprasgov/anual"

NOME_TABELA = {
    "VW_DM_PNCP_ITEM_RESULTADO": "pncp_item_resultado",
    "VW_FT_PNCP_COMPRA": "pncp_compra",
    "VW_FT_PNCP_COMPRA_ITEM": "pncp_compra_item",
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


def _garantir_schema_varchar(con: duckdb.DuckDBPyConnection, tabela: str):
    if not _tabela_existe(con, tabela):
        return
    tipos = con.execute(f"DESCRIBE lake.main.{tabela}").fetchall()
    colunas_erradas = [row[0] for row in tipos if row[1] != "VARCHAR" and row[0] != "ano"]
    if colunas_erradas:
        print(f"[{tabela}] Schema inválido em {colunas_erradas}. Descartando para recriar com tipos corretos.")
        con.execute(f"DROP TABLE lake.main.{tabela}")


def _carregar_ano(con: duckdb.DuckDBPyConnection, tabela: str, view: str, ano: int):
    url = f"{BASE_URL}/{ano}/comprasGOV-anual-{view}-{ano}.csv"
    print(f"[{tabela}] Carregando {ano}...")

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _staging AS
        SELECT *, {ano} AS ano
        FROM read_csv_auto('{url}', header=true, all_varchar=true)
    """)

    if not _tabela_existe(con, tabela):
        con.execute(f"CREATE TABLE lake.main.{tabela} AS SELECT * FROM _staging")
    else:
        lake_cols = {row[0] for row in con.execute(f"DESCRIBE lake.main.{tabela}").fetchall()}
        staging_cols = {row[0] for row in con.execute("DESCRIBE _staging").fetchall()}
        for col in staging_cols - lake_cols:
            con.execute(f'ALTER TABLE lake.main.{tabela} ADD COLUMN "{col}" VARCHAR')
            print(f"[{tabela}] Nova coluna detectada: {col}")
        con.execute(f"INSERT INTO lake.main.{tabela} BY NAME SELECT * FROM _staging")

    print(f"[{tabela}] Ano {ano} concluído.")


def _atualizar_view_dedup(con: duckdb.DuckDBPyConnection):
    if not _tabela_existe(con, "pncp_compra"):
        return
    con.execute("""
        CREATE OR REPLACE VIEW lake.main.pncp_compra_atual AS
        SELECT * EXCLUDE (rn) FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY cod_compra
                    ORDER BY data_atualizacao DESC, existe_resultado DESC
                ) AS rn
            FROM lake.main.pncp_compra
        ) WHERE rn = 1
    """)
    print("[pncp_compra_atual] View de deduplicação atualizada.")


def main(views: list[str], anos: range, caminho_meta: str, data_path: str, nome_segredo: str):
    con = ducklake.conectar(caminho_meta, data_path, nome_segredo)

    for view in views:
        tabela = NOME_TABELA[view]
        _garantir_schema_varchar(con, tabela)
        ja_carregados = _anos_carregados(con, tabela)

        for ano in anos:
            if ano in ja_carregados:
                print(f"[{tabela}] Ano {ano} já está no lake. Pulando.")
                continue
            try:
                _carregar_ano(con, tabela, view, ano)
            except Exception as e:
                print(f"[{tabela}] Erro ao carregar ano {ano}: {e}")

    _atualizar_view_dedup(con)
    ducklake.fechar(con, caminho_meta, nome_segredo)
