{{ config(
    materialized='view',
    database='lake',
    tags=['marts', 'margem_preferencia']
) }}

WITH explodido AS (
    SELECT ncm.codigo, d.*
    FROM {{ ref('int_margem__ncm_prefixos') }} AS d
    JOIN read_csv('s3://{{ var("bucket_lake") }}/ncm_prefixos.csv') AS ncm
        ON ncm.prefixo = d.prefixo_ncm
),

-- prioriza o prefixo mais específico: evita duplicar um NCM de 8 dígitos que também é coberto por
-- um prefixo mais genérico da mesma resolução (ex: cics_9 lista o capítulo "85" E o item "85044010")
enumerated AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY codigo, resolucao, data_inicio
        ORDER BY LENGTH(prefixo_ncm) DESC
    ) AS rn
    FROM explodido
),

dedup AS (
    SELECT * EXCLUDE (rn)
    FROM enumerated
    WHERE rn = 1
)

SELECT codigo, * EXCLUDE (codigo, prefixo_ncm)
FROM dedup
