import duckdb
import os
from pathlib import Path

from utils.constantes import RAIZ_PROJETO

DADOS = Path(RAIZ_PROJETO) / "dados"
DB_PATH = DADOS / "margem_preferencia" / "margem.duckdb"
RESULTS_PATH = DADOS
NCM_CSV_PATH = DADOS / "ncm_prefixos.csv"

con = duckdb.connect(DB_PATH)

con.execute(f"""
    -- Cruza a tabela de prefixos por período com a tabela de NCM pra ter o código completo do NCM
    CREATE OR REPLACE TABLE dim_margem_ncm AS
        SELECT ncm.codigo, d.* EXCLUDE prefixo_ncm
        FROM dim_margem_ncm_prefixos AS d
        JOIN '{NCM_CSV_PATH.as_posix()}' AS ncm
        ON ncm.prefixo = d.prefixo_ncm
""")

# Exporta a tabela criada pra um CSV utf-8, no mesmo lugar que o dbt já lê (dados/dim_margem_ncm_utf8.csv)
df = con.execute("SELECT * FROM dim_margem_ncm").df()
df.to_csv(os.path.join(RESULTS_PATH, "dim_margem_ncm_utf8.csv"), index=False, lineterminator="\n")

n = con.execute("SELECT COUNT(*) FROM dim_margem_ncm").fetchone()[0]
con.close()
print(f"OK: {n} registros gerados em dados/dim_margem_ncm_utf8.csv")
