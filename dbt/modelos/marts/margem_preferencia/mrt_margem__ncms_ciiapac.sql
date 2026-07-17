{{ config(
    materialized='view',
    database='lake',
    tags=['marts', 'margem_preferencia']
) }}

SELECT ncm.codigo AS ncm, d.* EXCLUDE (prefixo_ncm, ultima, ativa)
FROM {{ ref('int_margem__ncm_prefixos') }} AS d
JOIN read_csv('s3://{{ var("bucket_lake") }}/ncm_prefixos.csv') AS ncm
    ON ncm.prefixo = d.prefixo_ncm
WHERE starts_with(resolucao, 'ciiapac')
    AND ativa