import pyarrow as pa

import shared.ducklake as ducklake


NOME_TABELA = "ncm_gold"


def main(tabela: pa.Table, caminho_meta: str, data_path: str, nome_segredo: str):
    con = ducklake.conectar(caminho_meta, data_path, nome_segredo)
    con.register("ncm_temp", tabela)
    con.execute(f"CREATE OR REPLACE TABLE lake.main.{NOME_TABELA} AS SELECT * FROM ncm_temp")
    print(f"Tabela {NOME_TABELA} atualizada no lake.")
    ducklake.fechar(con, caminho_meta, nome_segredo)
