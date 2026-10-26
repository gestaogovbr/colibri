{{ config(
    materialized='view',
    tags=['staging', 'pncp', 'comprasgov', 'delog']
) }}

WITH raw_data AS (
    {{ pncp_comprasgov_delog_source('resultados') }}
)

SELECT
    * EXCLUDE (
        data_atualizacao_pncp
    ) REPLACE (
        TRY_CAST(
            quantidade_homologada AS DECIMAL(28,8)
        ) AS quantidade_homologada,

        TRY_CAST(
            valor_unitario_homologado AS DECIMAL(28,8)
        ) AS valor_unitario_homologado,

        TRY_CAST(
            valor_total_homologado AS DECIMAL(28,8)
        ) AS valor_total_homologado,

        TRY_CAST(
            percentual_desconto AS DECIMAL(28,8)
        ) AS percentual_desconto,

        TRY_CAST(
            valor_nominal_moeda_estrangeira AS DECIMAL(28,8)
        ) AS valor_nominal_moeda_estrangeira
    ),

    TRY_CAST(
        data_atualizacao_pncp AS DATE
    ) AS data_atualizacao

FROM raw_data