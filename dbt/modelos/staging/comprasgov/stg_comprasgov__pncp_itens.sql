{{ config(
    materialized='view',
    tags=['staging', 'pncp', 'comprasgov']
) }}

WITH raw_data AS (
    {{ comprasgov_source('itens') }}
)

SELECT
    * EXCLUDE (
        data_atualizacao_pncp,
        numero_item_pncp,
        item_categoria_id_pncp,
        criterio_julgamento_id_pncp
    ) REPLACE (
        TRY_CAST(
            quantidade AS DECIMAL(28,8)
        ) AS quantidade,

        TRY_CAST(
            valor_unitario_estimado AS DECIMAL(28,8)
        ) AS valor_unitario_estimado,

        TRY_CAST(
            valor_total AS DECIMAL(28,8)
        ) AS valor_total,

        TRY_CAST(
            valor_total_resultado AS DECIMAL(28,8)
        ) AS valor_total_resultado,

        TRY_CAST(
            valor_unitario_resultado AS DECIMAL(28,8)
        ) AS valor_unitario_resultado,

        TRY_CAST(
            quantidade_resultado AS DECIMAL(28,8)
        ) AS quantidade_resultado
    ),

    TRY_CAST(
        data_atualizacao_pncp AS DATE
    ) AS data_atualizacao,

    numero_item_pncp AS numero_item,
    item_categoria_id_pncp AS item_categoria_id,
    criterio_julgamento_id_pncp AS criterio_julgamento_id

FROM raw_data