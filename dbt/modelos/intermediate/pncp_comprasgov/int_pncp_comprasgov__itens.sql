{{ config(
    materialized='table',
    database='lake',
    tags=['intermediate', 'pncp', 'comprasgov']
) }}

WITH staging AS (
    SELECT * FROM {{ ref('stg_pncp_comprasgov__itens') }}
),

sem_duplicatas AS (
    SELECT * EXCLUDE (_dbt_loaded_at, granularidade, periodo)
    FROM staging
    GROUP BY ALL
),

dedup AS (
    SELECT * EXCLUDE (_rn)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY id_compra_item, data_atualizacao
                ORDER BY {{ contar_colunas_preenchidas(
                    ref('stg_pncp_comprasgov__itens'),
                    excluir=['id_compra_item', 'data_atualizacao', 'granularidade', 'periodo', '_dbt_loaded_at']
                ) }} DESC
            ) AS _rn
        FROM sem_duplicatas
    )
    WHERE _rn = 1
),

scd2 AS (
    SELECT
        *,
        data_atualizacao                             AS valido_de,
        LEAD(data_atualizacao) OVER (
            PARTITION BY id_compra_item
            ORDER BY data_atualizacao
        ) - 1                                        AS valido_ate,
        LEAD(data_atualizacao) OVER (
            PARTITION BY id_compra_item
            ORDER BY data_atualizacao
        ) IS NULL                                    AS is_current
    FROM dedup
)

SELECT * FROM scd2
