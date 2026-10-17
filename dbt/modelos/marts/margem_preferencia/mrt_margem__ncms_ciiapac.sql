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
-- Prefixos de tamanhos diferentes podem cobrir o mesmo NCM (ex. 850440 e
-- 85044010): fica uma linha por NCM, com tecnologia nacional à frente de
-- conteúdo nacional e, no empate, o prefixo mais específico (issue 106).
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ncm.codigo
    ORDER BY d.tipo_margem = 'tecnologia_nacional' DESC, length(d.prefixo_ncm) DESC
) = 1
