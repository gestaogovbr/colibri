import duckdb
import pyarrow as pa


SQL = """
WITH capitulo AS (
  SELECT n_base.Codigo AS prefixo, n_cross.Codigo AS codigo, n_cross.Descricao AS descricao,
         n_cross.Nivel AS nivel, n_base.Capitulo_Codigo AS capitulo_codigo,
         n_base.Capitulo_Descricao AS capitulo_descricao, n_cross.Caminho AS caminho
  FROM ncm_raw AS n_base JOIN ncm_raw AS n_cross ON n_base.Codigo = LEFT(n_cross.Codigo, 2)
  WHERE LENGTH(n_base.Codigo) = 2 AND LENGTH(n_cross.Codigo) = 8
),
posicao AS (
  SELECT n_base.Codigo AS prefixo, n_cross.Codigo AS codigo, n_cross.Descricao AS descricao,
         n_cross.Nivel AS nivel, n_base.Capitulo_Codigo AS capitulo_codigo,
         n_base.Capitulo_Descricao AS capitulo_descricao, n_cross.Caminho AS caminho
  FROM ncm_raw AS n_base JOIN ncm_raw AS n_cross ON n_base.Codigo = LEFT(n_cross.Codigo, 4)
  WHERE LENGTH(n_base.Codigo) = 4 AND LENGTH(n_cross.Codigo) = 8
),
subposicao_1 AS (
  SELECT n_base.Codigo AS prefixo, n_cross.Codigo AS codigo, n_cross.Descricao AS descricao,
         n_cross.Nivel AS nivel, n_base.Capitulo_Codigo AS capitulo_codigo,
         n_base.Capitulo_Descricao AS capitulo_descricao, n_cross.Caminho AS caminho
  FROM ncm_raw AS n_base JOIN ncm_raw AS n_cross ON n_base.Codigo = LEFT(n_cross.Codigo, 5)
  WHERE LENGTH(n_base.Codigo) = 5 AND LENGTH(n_cross.Codigo) = 8
),
subposicao_2 AS (
  SELECT n_base.Codigo AS prefixo, n_cross.Codigo AS codigo, n_cross.Descricao AS descricao,
         n_cross.Nivel AS nivel, n_base.Capitulo_Codigo AS capitulo_codigo,
         n_base.Capitulo_Descricao AS capitulo_descricao, n_cross.Caminho AS caminho
  FROM ncm_raw AS n_base JOIN ncm_raw AS n_cross ON n_base.Codigo = LEFT(n_cross.Codigo, 6)
  WHERE LENGTH(n_base.Codigo) = 6 AND LENGTH(n_cross.Codigo) = 8
),
item AS (
  SELECT n_base.Codigo AS prefixo, n_cross.Codigo AS codigo, n_cross.Descricao AS descricao,
         n_cross.Nivel AS nivel, n_base.Capitulo_Codigo AS capitulo_codigo,
         n_base.Capitulo_Descricao AS capitulo_descricao, n_cross.Caminho AS caminho
  FROM ncm_raw AS n_base JOIN ncm_raw AS n_cross ON n_base.Codigo = LEFT(n_cross.Codigo, 7)
  WHERE LENGTH(n_base.Codigo) = 7 AND LENGTH(n_cross.Codigo) = 8
),
subitem AS (
  SELECT n_base.Codigo AS prefixo, n_cross.Codigo AS codigo, n_cross.Descricao AS descricao,
         n_cross.Nivel AS nivel, n_base.Capitulo_Codigo AS capitulo_codigo,
         n_base.Capitulo_Descricao AS capitulo_descricao, n_cross.Caminho AS caminho
  FROM ncm_raw AS n_base JOIN ncm_raw AS n_cross ON n_base.Codigo = LEFT(n_cross.Codigo, 8)
  WHERE LENGTH(n_base.Codigo) = 8 AND LENGTH(n_cross.Codigo) = 8
)
SELECT * FROM capitulo
UNION ALL SELECT * FROM posicao
UNION ALL SELECT * FROM subposicao_1
UNION ALL SELECT * FROM subposicao_2
UNION ALL SELECT * FROM item
UNION ALL SELECT * FROM subitem
"""


def gerar_prefixos(enriquecido: list[dict]) -> pa.Table:
    # Remove pontos dos códigos para os JOINs por prefixo funcionarem
    sem_pontos = [{**r, "Codigo": r["Codigo"].replace(".", "")} for r in enriquecido]
    con = duckdb.connect()
    con.register("ncm_raw", pa.Table.from_pylist(sem_pontos))
    return con.execute(SQL).fetch_arrow_table()
