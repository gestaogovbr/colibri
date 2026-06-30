{{ config(
    materialized='table',
    database='lake',
    tags=['intermediate', 'pncp_em_numeros']
) }}

WITH staging AS (
    SELECT * FROM {{ ref('stg_pncp_em_numeros__agg_compra') }}
),

dedup AS (
    SELECT * EXCLUDE (_source_file)
    FROM staging
    GROUP BY ALL
),

scd2 AS (
    SELECT
        * EXCLUDE (_dbt_loaded_at),
        _dbt_loaded_at::TIMESTAMP                    AS valido_de,
        LEAD(_dbt_loaded_at::TIMESTAMP) OVER (
            PARTITION BY codigo
            ORDER BY _dbt_loaded_at
        ) - INTERVAL '1 day'                        AS valido_ate,
        LEAD(_dbt_loaded_at::TIMESTAMP) OVER (
            PARTITION BY codigo
            ORDER BY _dbt_loaded_at
        ) IS NULL                                   AS is_current
    FROM dedup
)

SELECT * FROM scd2
