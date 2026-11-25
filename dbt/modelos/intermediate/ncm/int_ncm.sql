{{ config(
    materialized='table',
    database='lake',
    tags=['intermediate', 'ncm']
) }}

WITH staging AS (
    SELECT * FROM {{ ref('stg_ncm') }}
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
        -- valido_de é TIMESTAMP (momento da carga): subtrair 1 dia, como nos modelos com
        -- data_atualizacao DATE, faria valido_ate < valido_de quando duas cargas ocorrem
        -- a menos de 24h. 1 microssegundo (a precisão do TIMESTAMP) mantém o fim inclusivo.
        ) - INTERVAL '1 microsecond'                AS valido_ate,
        LEAD(_dbt_loaded_at::TIMESTAMP) OVER (
            PARTITION BY codigo
            ORDER BY _dbt_loaded_at
        ) IS NULL                                   AS is_current
    FROM dedup
)

SELECT * FROM scd2
