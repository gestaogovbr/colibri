{{ config(
    materialized='view',
    database='lake',
    tags=['intermediate', 'pncp', 'comprasgov']
) }}

WITH origem AS (
    SELECT *
    FROM {{ ref('stg_comprasgov__pncp_itens') }}
),

historico AS (
    SELECT
        *,
        data_atualizacao AS valido_de,

        LEAD(data_atualizacao) OVER (
            PARTITION BY id_compra_item
            ORDER BY data_atualizacao
        ) AS valido_ate

    FROM origem
),

resultado AS (
    SELECT
        * REPLACE (
            TRY_CAST(
                TRY_CAST(ano_compra AS DOUBLE)
                AS INTEGER
            ) AS ano_compra
        ),
        valido_ate IS NULL AS is_current
    FROM historico
)

SELECT *
FROM resultado
