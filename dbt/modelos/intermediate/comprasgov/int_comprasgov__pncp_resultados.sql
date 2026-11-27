{{ config(
    materialized='view',
    database='lake',
    tags=['intermediate', 'pncp', 'comprasgov']
) }}

WITH origem AS (
    SELECT *
    FROM {{ ref('stg_comprasgov__pncp_resultados') }}
),

historico AS (
    SELECT
        *,
        data_atualizacao AS valido_de,

        LEAD(data_atualizacao) OVER (
            PARTITION BY id_compra_item, sequencial_resultado
            ORDER BY data_atualizacao
        ) AS valido_ate

    FROM origem
)

SELECT
    *,
    valido_ate IS NULL AS is_current
FROM historico