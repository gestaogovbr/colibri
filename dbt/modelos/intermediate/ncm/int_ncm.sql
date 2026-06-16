{{ config(
    materialized='table',
    database='lake',
    tags=['intermediate', 'ncm']
) }}

WITH staging AS (
    SELECT * FROM {{ ref('stg_ncm') }}
),

versioned AS (
    SELECT
        * EXCLUDE (_dbt_loaded_at),
        STRPTIME(
            REGEXP_EXTRACT(_source_file, 'ncm_silver_(\d{4}-\d{2}-\d{2}-\d{6})', 1),
            '%Y-%m-%d-%H%M%S'
        ) AS extraido_em
    FROM staging
),

scd2 AS (
    SELECT
        * EXCLUDE (_source_file, extraido_em),
        extraido_em AS valido_de,
        LEAD(extraido_em) OVER (
            PARTITION BY codigo
            ORDER BY extraido_em
        ) - INTERVAL '1 second'                     AS valido_ate,
        LEAD(extraido_em) OVER (
            PARTITION BY codigo
            ORDER BY extraido_em
        ) IS NULL                                    AS is_current
    FROM versioned
)

SELECT * FROM scd2
