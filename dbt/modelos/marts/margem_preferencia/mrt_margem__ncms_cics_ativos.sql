{{ config(
    materialized='view',
    database='lake',
    tags=['marts', 'margem_preferencia']
) }}

SELECT *
FROM {{ ref('mrt_margem__ncms_cics') }}
WHERE ativa
