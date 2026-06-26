{{ config(
    materialized='view',
    database='lake',
    tags=['marts', 'pncp', 'comprasgov']
) }}

WITH intermediate AS (
    SELECT * FROM {{ ref('int_pncp_comprasgov__compras') }}
)

SELECT * EXCLUDE(is_current)
FROM intermediate
WHERE is_current = true