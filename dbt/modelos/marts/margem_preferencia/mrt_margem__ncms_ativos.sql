{{ config(
    materialized='view',
    database='lake',
    tags=['marts', 'margem_preferencia']
) }}

-- Idêntica à mrt_margem__ncms, mas só com os NCMs que estão com margem ativa hoje
SELECT *
FROM {{ ref('mrt_margem__ncms') }}
WHERE ativa
