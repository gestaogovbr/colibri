{{ config(
    materialized='view',
    tags=['intermediate', 'pncp', 'comprasgov']
) }}

WITH origem AS (

    SELECT *
    FROM {{ ref('stg_comprasgov__pncp_compras') }}

),

historico AS (

    SELECT
        *,

        data_atualizacao AS valido_de,

        LEAD(data_atualizacao) OVER (
            PARTITION BY cod_compra
            ORDER BY data_atualizacao
        ) AS valido_ate

    FROM origem

)

SELECT
    *,
    valido_ate IS NULL AS is_current

FROM historico