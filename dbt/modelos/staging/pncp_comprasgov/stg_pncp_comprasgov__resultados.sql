/*
Modelo Staging: Consolidação de todos os resultados de itens de compras públicas
Empilha todos os arquivos PNCP_ITEM_RESULTADO de diferentes anos e harmoniza colunas
renomeadas com sufixo _pncp (alteração de schema em 2025).
*/

{{ config(
    materialized='table',
    database='lake',
    tags=['staging', 'pncp', 'comprasgov']
) }}

WITH raw_data AS (
  {{ pncp_comprasgov_union_por_ano('VW_DM_PNCP_ITEM_RESULTADO') }}
),

bronze AS (
    SELECT
        *,
        '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' AS _dbt_loaded_at
    FROM raw_data
)

SELECT
    * EXCLUDE (
        data_atualizacao, data_atualizacao_pncp,
        data_inclusao, data_inclusao_pncp,
        numero_item, numero_item_pncp,
        data_cancelamento, data_cancelamento_pncp,
        data_resultado, data_resultado_pncp
    ),

    -- Colunas com variante _pncp: COALESCE prioriza a versão não-pncp (mais completa no diário)
    CAST(COALESCE(data_atualizacao, data_atualizacao_pncp) AS DATE) AS data_atualizacao,
    COALESCE(data_inclusao,    data_inclusao_pncp)    AS data_inclusao,
    COALESCE(numero_item,      numero_item_pncp)      AS numero_item,
    COALESCE(data_cancelamento, data_cancelamento_pncp) AS data_cancelamento,
    COALESCE(data_resultado,   data_resultado_pncp)   AS data_resultado
FROM bronze
