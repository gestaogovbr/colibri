{{ config(
    materialized='table',
    database='lake',
    tags=['intermediate', 'ncm']
) }}

-- Tabela de lookup: para cada prefixo NCM (capítulo/posição/subposição/item),
-- lista todos os subitens (8 dígitos) que pertencem a ele.
-- Porta a lógica de gold/transform_prefixos.py para SQL puro.

WITH ncm_raw AS (
    SELECT
        REPLACE(codigo, '.', '') AS codigo,
        descricao,
        nivel,
        capitulo_codigo,
        capitulo_descricao,
        caminho
    FROM {{ ref('int_ncm') }}
    WHERE is_current
),

capitulo AS (
    SELECT n_base.codigo AS prefixo, n_cross.codigo AS codigo,
           n_cross.descricao AS descricao, n_cross.nivel AS nivel,
           n_base.capitulo_codigo AS capitulo_codigo,
           n_base.capitulo_descricao AS capitulo_descricao,
           n_cross.caminho AS caminho
    FROM ncm_raw AS n_base
    JOIN ncm_raw AS n_cross ON n_base.codigo = LEFT(n_cross.codigo, 2)
    WHERE LENGTH(n_base.codigo) = 2 AND LENGTH(n_cross.codigo) = 8
),

posicao AS (
    SELECT n_base.codigo AS prefixo, n_cross.codigo AS codigo,
           n_cross.descricao AS descricao, n_cross.nivel AS nivel,
           n_base.capitulo_codigo AS capitulo_codigo,
           n_base.capitulo_descricao AS capitulo_descricao,
           n_cross.caminho AS caminho
    FROM ncm_raw AS n_base
    JOIN ncm_raw AS n_cross ON n_base.codigo = LEFT(n_cross.codigo, 4)
    WHERE LENGTH(n_base.codigo) = 4 AND LENGTH(n_cross.codigo) = 8
),

subposicao_1 AS (
    SELECT n_base.codigo AS prefixo, n_cross.codigo AS codigo,
           n_cross.descricao AS descricao, n_cross.nivel AS nivel,
           n_base.capitulo_codigo AS capitulo_codigo,
           n_base.capitulo_descricao AS capitulo_descricao,
           n_cross.caminho AS caminho
    FROM ncm_raw AS n_base
    JOIN ncm_raw AS n_cross ON n_base.codigo = LEFT(n_cross.codigo, 5)
    WHERE LENGTH(n_base.codigo) = 5 AND LENGTH(n_cross.codigo) = 8
),

subposicao_2 AS (
    SELECT n_base.codigo AS prefixo, n_cross.codigo AS codigo,
           n_cross.descricao AS descricao, n_cross.nivel AS nivel,
           n_base.capitulo_codigo AS capitulo_codigo,
           n_base.capitulo_descricao AS capitulo_descricao,
           n_cross.caminho AS caminho
    FROM ncm_raw AS n_base
    JOIN ncm_raw AS n_cross ON n_base.codigo = LEFT(n_cross.codigo, 6)
    WHERE LENGTH(n_base.codigo) = 6 AND LENGTH(n_cross.codigo) = 8
),

item AS (
    SELECT n_base.codigo AS prefixo, n_cross.codigo AS codigo,
           n_cross.descricao AS descricao, n_cross.nivel AS nivel,
           n_base.capitulo_codigo AS capitulo_codigo,
           n_base.capitulo_descricao AS capitulo_descricao,
           n_cross.caminho AS caminho
    FROM ncm_raw AS n_base
    JOIN ncm_raw AS n_cross ON n_base.codigo = LEFT(n_cross.codigo, 7)
    WHERE LENGTH(n_base.codigo) = 7 AND LENGTH(n_cross.codigo) = 8
),

subitem AS (
    SELECT n_base.codigo AS prefixo, n_cross.codigo AS codigo,
           n_cross.descricao AS descricao, n_cross.nivel AS nivel,
           n_base.capitulo_codigo AS capitulo_codigo,
           n_base.capitulo_descricao AS capitulo_descricao,
           n_cross.caminho AS caminho
    FROM ncm_raw AS n_base
    JOIN ncm_raw AS n_cross ON n_base.codigo = LEFT(n_cross.codigo, 8)
    WHERE LENGTH(n_base.codigo) = 8 AND LENGTH(n_cross.codigo) = 8
)

SELECT * FROM capitulo
UNION ALL SELECT * FROM posicao
UNION ALL SELECT * FROM subposicao_1
UNION ALL SELECT * FROM subposicao_2
UNION ALL SELECT * FROM item
UNION ALL SELECT * FROM subitem
