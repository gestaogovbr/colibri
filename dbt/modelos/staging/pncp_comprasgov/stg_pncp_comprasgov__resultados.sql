/*
Modelo Staging: Consolidação de todos os resultados de itens de compras públicas.
Empilha arquivos VW_DM_PNCP_ITEM_RESULTADO de todas as granularidades e harmoniza
colunas renomeadas com sufixo _pncp (alteração de schema em 2025), como nos modelos
de compras e itens.
*/

{{ config(
    materialized='incremental',
    incremental_strategy='append',
    database='lake',
    tags=['staging', 'pncp', 'comprasgov']
) }}

WITH raw_data AS (
  {{ pncp_comprasgov_source('VW_DM_PNCP_ITEM_RESULTADO') }}
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
        data_inclusao, data_inclusao_pncp,
        data_cancelamento, data_cancelamento_pncp,
        data_resultado, data_resultado_pncp,
        data_atualizacao, data_atualizacao_pncp
    ),

    -- Colunas com variante _pncp: os arquivos antigos preenchem só a coluna original e os novos
    -- (anual 2025+, mensal 2025-08+, diário 2025-08-28+) só a variante _pncp. As variantes chegam
    -- tipadas (BIGINT/TIMESTAMP/DATE) e o COALESCE exige tipos iguais, daí os CASTs.
    COALESCE(CAST(numero_item       AS VARCHAR), CAST(numero_item_pncp       AS VARCHAR)) AS numero_item,
    COALESCE(CAST(data_inclusao     AS VARCHAR), CAST(data_inclusao_pncp     AS VARCHAR)) AS data_inclusao,
    COALESCE(CAST(data_cancelamento AS VARCHAR), CAST(data_cancelamento_pncp AS VARCHAR)) AS data_cancelamento,
    COALESCE(CAST(data_resultado    AS VARCHAR), CAST(data_resultado_pncp    AS VARCHAR)) AS data_resultado,
    COALESCE(CAST(data_atualizacao  AS DATE),    CAST(data_atualizacao_pncp  AS DATE))    AS data_atualizacao
FROM bronze

{% if is_incremental() %}
WHERE (granularidade, periodo) IN (
    SELECT granularidade, periodo
    FROM read_csv(
        '../dados/alteracoes/pncp_comprasgov_alteracoes.csv',
        header = true,
        columns = {'view': 'VARCHAR', 'granularidade': 'VARCHAR', 'periodo': 'VARCHAR'}
    )
    WHERE view = 'VW_DM_PNCP_ITEM_RESULTADO'
)
{% endif %}
