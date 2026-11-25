{{ config(
    materialized='table',
    database='lake',
    tags=['intermediate', 'uasg']
) }}

{{ scd2_por_snapshot(ref('stg_uasg__orgaos'), chave='codigo_orgao') }}
