import json
import csv
import duckdb


with open("ncm.json", encoding="utf-8") as f:
    nomenclaturas = json.load(f)

with open("ncm/ncm_raw.csv", "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.DictWriter(f, fieldnames=nomenclaturas[0].keys())
    writer.writeheader()
    for item in nomenclaturas:
        writer.writerow({**item, "Codigo": item["Codigo"].replace(".", "")})

print(f"{len(nomenclaturas)} registros exportados para ncm/ncm_raw.csv")

duckdb.sql(
    """
COPY (
  WITH capitulo AS (
    SELECT
      n_base.Codigo AS prefixo,
      n_cross.Codigo AS codigo,
      n_base.Descricao AS descricao,
      n_base.Nivel AS nivel,
      n_base.Capitulo_codigo AS capitulo_codigo,
      n_base.Capitulo_descricao AS capitulo_descricao,
      n_cross.Caminho AS caminho
    FROM 'ncm/ncm_raw.csv' AS n_base
    JOIN 'ncm/ncm_raw.csv' AS n_cross
      ON n_base.Codigo = LEFT(n_cross.Codigo, 2)
    WHERE LENGTH(n_base.Codigo) = 2
      AND LENGTH(n_cross.Codigo) = 8
  ),
  posicao AS (
    SELECT
      n_base.Codigo AS prefixo,
      n_cross.Codigo AS codigo,
      n_base.Descricao AS descricao,
      n_base.Nivel AS nivel,
      n_base.Capitulo_codigo AS capitulo_codigo,
      n_base.Capitulo_descricao AS capitulo_descricao,
      n_cross.Caminho AS caminho
    FROM 'ncm/ncm_raw.csv' AS n_base
    JOIN 'ncm/ncm_raw.csv' AS n_cross
      ON n_base.Codigo = LEFT(n_cross.Codigo, 4)
    WHERE LENGTH(n_base.Codigo) = 4
      AND LENGTH(n_cross.Codigo) = 8
  ),
  subposicao_1 AS (
    SELECT
      n_base.Codigo AS prefixo,
      n_cross.Codigo AS codigo,
      n_base.Descricao AS descricao,
      n_base.Nivel AS nivel,
      n_base.Capitulo_codigo AS capitulo_codigo,
      n_base.Capitulo_descricao AS capitulo_descricao,
      n_cross.Caminho AS caminho
    FROM 'ncm/ncm_raw.csv' AS n_base
    JOIN 'ncm/ncm_raw.csv' AS n_cross
      ON n_base.Codigo = LEFT(n_cross.Codigo, 5)
    WHERE LENGTH(n_base.Codigo) = 5
      AND LENGTH(n_cross.Codigo) = 8
  ),
  subposicao_2 AS (
    SELECT
      n_base.Codigo AS prefixo,
      n_cross.Codigo AS codigo,
      n_base.Descricao AS descricao,
      n_base.Nivel AS nivel,
      n_base.Capitulo_codigo AS capitulo_codigo,
      n_base.Capitulo_descricao AS capitulo_descricao,
      n_cross.Caminho AS caminho
    FROM 'ncm/ncm_raw.csv' AS n_base
    JOIN 'ncm/ncm_raw.csv' AS n_cross
      ON n_base.Codigo = LEFT(n_cross.Codigo, 6)
    WHERE LENGTH(n_base.Codigo) = 6
      AND LENGTH(n_cross.Codigo) = 8
  ),
  item AS (
    SELECT
      n_base.Codigo AS prefixo,
      n_cross.Codigo AS codigo,
      n_base.Descricao AS descricao,
      n_base.Nivel AS nivel,
      n_base.Capitulo_codigo AS capitulo_codigo,
      n_base.Capitulo_descricao AS capitulo_descricao,
      n_cross.Caminho AS caminho
    FROM 'ncm/ncm_raw.csv' AS n_base
    JOIN 'ncm/ncm_raw.csv' AS n_cross
      ON n_base.Codigo = LEFT(n_cross.Codigo, 7)
    WHERE LENGTH(n_base.Codigo) = 7
      AND LENGTH(n_cross.Codigo) = 8
  ),
  subitem AS (
    SELECT
      n_base.Codigo AS prefixo,
      n_cross.Codigo AS codigo,
      n_base.Descricao AS descricao,
      n_base.Nivel AS nivel,
      n_base.Capitulo_codigo AS capitulo_codigo,
      n_base.Capitulo_descricao AS capitulo_descricao,
      n_cross.Caminho AS caminho
    FROM 'ncm/ncm_raw.csv' AS n_base
    JOIN 'ncm/ncm_raw.csv' AS n_cross
      ON n_base.Codigo = LEFT(n_cross.Codigo, 8)
    WHERE LENGTH(n_base.Codigo) = 8
      AND LENGTH(n_cross.Codigo) = 8
  ),
  agreg AS (
    SELECT * FROM capitulo
    UNION ALL SELECT * FROM posicao
    UNION ALL SELECT * FROM subposicao_1
    UNION ALL SELECT * FROM subposicao_2
    UNION ALL SELECT * FROM item
    UNION ALL SELECT * FROM subitem
  )
  SELECT * FROM agreg
) TO 'ncm/ncm.csv' (HEADER, DELIMITER ',');
"""
)

print("ncm.csv gerado com sucesso.")
