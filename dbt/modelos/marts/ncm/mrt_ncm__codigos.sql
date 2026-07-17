{{ config(
    materialized='view',
    database='lake',
    tags=['marts', 'ncm']
) }}

-- Um registro por código NCM de 8 dígitos
SELECT * EXCLUDE (nivel)
FROM {{ ref('int_ncm__prefixos') }}
WHERE nivel = 'Subitem'
