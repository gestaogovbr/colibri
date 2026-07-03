{{ config(
    materialized='table',
    database='lake',
    tags=['staging', 'margem']
) }}

-- Cruza a tabela de prefixos por período com a tabela de NCM pra ter o código completo do NCM
SELECT ncm.codigo, d.* EXCLUDE (prefixo_ncm)
FROM {{ ref('stg_dim_margem_ncm_prefixos') }} AS d
JOIN read_csv('../dados/ncm_prefixos.csv') AS ncm
    ON ncm.prefixo = d.prefixo_ncm
