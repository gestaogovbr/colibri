# Cria tabela dim_margem_resolucoes a partir do CSV resolucoes_CICS.csv

import duckdb
import os
from pathlib import Path

from utils.constantes import RAIZ_PROJETO

DADOS = Path(RAIZ_PROJETO) / "dados" / "margem_preferencia"
DB_PATH = DADOS / "margem.duckdb"
RESULTS_PATH = DADOS
CSV_PATH = DADOS / "CICS" / "resolucoes_CICS.csv"

con = duckdb.connect(DB_PATH)

# Remove tabelas antigas
con.execute("DROP TABLE IF EXISTS dim_margem_ncm_prefixos")
con.execute("DROP TABLE IF EXISTS fato_margem_eventos")
con.execute("DROP TABLE IF EXISTS dim_margem_resolucoes")

# Lê o CSV e cria a tabela de resoluções
con.execute(f"""
    CREATE TABLE dim_margem_resolucoes AS
    SELECT * FROM read_csv('{CSV_PATH.as_posix()}', delim=';', auto_detect=true)
""")

# Adiciona id pra cada resolução
con.execute("ALTER TABLE dim_margem_resolucoes ADD PRIMARY KEY (id)")

# Exporta a tabela criada pra um CSV utf-8
df = con.execute("SELECT * FROM dim_margem_resolucoes").df()
df.to_csv(os.path.join(RESULTS_PATH, "dim_margem_resolucoes_utf8.csv"), index=False, lineterminator="\n")

# Puxa numero de resoluções pro log
n = con.execute("SELECT COUNT(*) FROM dim_margem_resolucoes").fetchone()[0]
con.close()
print(f"OK: {n} resolucoes inseridas")
