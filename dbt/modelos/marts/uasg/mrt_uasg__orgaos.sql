{{ config(
    materialized='view',
    database='lake',
    tags=['marts', 'uasg']
) }}

SELECT * EXCLUDE (is_current, valido_ate)
FROM {{ ref('int_uasg__orgaos') }}
WHERE is_current
