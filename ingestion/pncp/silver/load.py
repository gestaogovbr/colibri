from datetime import datetime

import duckdb

import shared.ducklake as ducklake


BASE_URL = "https://repositorio.dados.gov.br/seges/comprasgov/anual"

NOME_TABELA = {
    "VW_FT_PNCP_COMPRA": "pncp_compra",
}

CHAVE_TABELA = {
    "pncp_compra": "cod_compra",
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
        print(f"[{tabela}] Schema inválido em {colunas_erradas}. Descartando para recriar.")
        con.execute(f"DROP TABLE lake.main.{tabela}")


def _staging(con: duckdb.DuckDBPyConnection, chave: str | None, url: str, ano: int):
    if chave:
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE _staging AS
            SELECT * EXCLUDE (rn) FROM (
                SELECT *, {ano} AS ano,
                    row_number() OVER (
                        PARTITION BY {chave}
                        ORDER BY existe_resultado DESC NULLS LAST
                    ) AS rn
                FROM read_csv_auto('{url}', header=true, all_varchar=true)
            ) WHERE rn = 1
        """)
    else:
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE _staging AS
            SELECT *, {ano} AS ano
            FROM read_csv_auto('{url}', header=true, all_varchar=true)
        """)


def _evoluir_schema(con: duckdb.DuckDBPyConnection, tabela: str):
    lake_cols = {row[0] for row in con.execute(f"DESCRIBE lake.main.{tabela}").fetchall()}
    staging_cols = {row[0] for row in con.execute("DESCRIBE _staging").fetchall()}
    for col in staging_cols - lake_cols:
        con.execute(f'ALTER TABLE lake.main.{tabela} ADD COLUMN "{col}" VARCHAR')
        print(f"[{tabela}] Nova coluna detectada: {col}")


def _carregar_ano(con: duckdb.DuckDBPyConnection, tabela: str, chave: str | None, view: str, ano: int):
    url = f"{BASE_URL}/{ano}/comprasGOV-anual-{view}-{ano}.csv"
    print(f"[{tabela}] Carregando {ano}...")
    _staging(con, chave, url, ano)

    if not _tabela_existe(con, tabela):
        con.execute(f"CREATE TABLE lake.main.{tabela} AS SELECT * FROM _staging")
    else:
        _evoluir_schema(con, tabela)
        if chave:
            con.execute(f"""
                DELETE FROM lake.main.{tabela}
                WHERE {chave} IN (SELECT {chave} FROM _staging WHERE {chave} IS NOT NULL)
            """)
        con.execute(f"INSERT INTO lake.main.{tabela} BY NAME SELECT * FROM _staging")

    print(f"[{tabela}] Ano {ano} concluído.")


def _upsert_ano_corrente(con: duckdb.DuckDBPyConnection, tabela: str, chave: str, view: str, ano: int):
    """Atualiza só o que mudou no ano corrente, comparando data_atualizacao"""
    url = f"{BASE_URL}/{ano}/comprasGOV-anual-{view}-{ano}.csv"
    print(f"[{tabela}] Verificando mudanças em {ano}...")
    _staging(con, chave, url, ano)
    _evoluir_schema(con, tabela)

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _changed AS
        SELECT s.* FROM _staging s
        LEFT JOIN lake.main.{tabela} t USING ({chave})
        WHERE t.{chave} IS NULL
           OR t.data_atualizacao IS DISTINCT FROM s.data_atualizacao
    """)

    n = con.execute("SELECT count(*) FROM _changed").fetchone()[0]
    if n == 0:
        print(f"[{tabela}] Ano {ano}: sem mudanças.")
        return

    con.execute(f"""
        DELETE FROM lake.main.{tabela}
        WHERE {chave} IN (SELECT {chave} FROM _changed)
    """)
    con.execute(f"INSERT INTO lake.main.{tabela} BY NAME SELECT * FROM _changed")
    print(f"[{tabela}] Ano {ano}: {n} registro(s) novo(s)/atualizado(s).")


def main(views: list[str], anos: range, caminho_meta: str, data_path: str, nome_segredo: str):
    ano_corrente = datetime.now().year
    con = ducklake.conectar(caminho_meta, data_path, nome_segredo)

    for view in views:
        tabela = NOME_TABELA[view]
        chave = CHAVE_TABELA[tabela]
        _garantir_schema_varchar(con, tabela)
        ja_carregados = _anos_carregados(con, tabela)

        for ano in anos:
            if ano in ja_carregados and ano < ano_corrente:
                print(f"[{tabela}] Ano {ano} já está no lake. Pulando.")
                continue
            if ano in ja_carregados and ano == ano_corrente:
                try:
                    _upsert_ano_corrente(con, tabela, chave, view, ano)
                except Exception as e:
                    print(f"[{tabela}] Erro ao atualizar ano {ano}: {e}")
                continue
            try:
                _carregar_ano(con, tabela, chave, view, ano)
            except Exception as e:
                print(f"[{tabela}] Erro ao carregar ano {ano}: {e}")

    ducklake.fechar(con, caminho_meta, nome_segredo)
