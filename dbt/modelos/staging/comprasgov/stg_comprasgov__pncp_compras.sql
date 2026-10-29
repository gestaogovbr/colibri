{{ config(
    materialized='view',
    tags=['staging', 'pncp', 'comprasgov']
) }}

WITH raw_data AS (
    {{ comprasgov_source('compras') }}
)

SELECT
    * EXCLUDE (
        data_atualizacao_pncp,
        modalidade_id_pncp,
        modo_disputa_id_pncp,
        ano_compra_pncp,
        sequencial_compra_pncp
    ) REPLACE (
        TRY_CAST(
            valor_total_estimado AS DECIMAL(28,8)
        ) AS valor_total_estimado
    ),

    TRY_CAST(
        data_atualizacao_pncp AS DATE
    ) AS data_atualizacao,

    modalidade_id_pncp AS modalidade_id,
    modo_disputa_id_pncp AS modo_disputa_id,
    ano_compra_pncp AS ano_compra,
    sequencial_compra_pncp AS sequencial_compra

FROM raw_data