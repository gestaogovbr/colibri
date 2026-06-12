/*
Modelo Staging: Consolidação de todos os itens de compras públicas
Empilha todos os arquivos PNCP_COMPRA_ITEM de diferentes anos e harmoniza colunas
renomeadas com sufixo _pncp (alteração de schema em 2025).
*/

{{ config(
    materialized='table',
    database='lake',
    tags=['staging', 'pncp', 'comprasgov']
) }}

WITH raw_data AS (
  {{ pncp_comprasgov_union_por_ano('VW_FT_PNCP_COMPRA_ITEM') }}
),

bronze AS (
    SELECT
        *,
        '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' AS _dbt_loaded_at
    FROM raw_data
)

SELECT
    * EXCLUDE (
        numero_item, numero_item_pncp,
        item_categoria_id, item_categoria_id_pncp,
        criterio_julgamento_id, criterio_julgamento_id_pncp,
        data_inclusao, data_inclusao_pncp,
        data_atualizacao, data_atualizacao_pncp
    ),

    -- Colunas com variante _pncp: COALESCE prioriza a versão não-pncp (mais completa no diário)
    COALESCE(numero_item,          numero_item_pncp)          AS numero_item,
    COALESCE(item_categoria_id,    item_categoria_id_pncp)    AS item_categoria_id,
    COALESCE(criterio_julgamento_id, criterio_julgamento_id_pncp) AS criterio_julgamento_id,
    COALESCE(data_inclusao,        data_inclusao_pncp)        AS data_inclusao,
    CAST(COALESCE(data_atualizacao, data_atualizacao_pncp) AS DATE) AS data_atualizacao
FROM bronze
