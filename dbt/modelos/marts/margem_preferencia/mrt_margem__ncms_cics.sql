{{ config(
    materialized='view',
    database='lake',
    tags=['marts', 'margem_preferencia']
) }}

SELECT ncm.codigo AS ncm, d.* EXCLUDE (prefixo_ncm, ultima, ativa)
FROM {{ ref('int_margem__ncm_prefixos') }} AS d
JOIN {{ ref('int_ncm_prefixos') }} AS ncm
    ON ncm.prefixo = d.prefixo_ncm
WHERE starts_with(resolucao, 'cics')
    AND ativa