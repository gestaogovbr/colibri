/*
Modelo Staging: Consolidação de todos os resultados de itens de compras públicas.
Empilha arquivos VW_DM_PNCP_ITEM_RESULTADO de todas as granularidades.
Schema estável entre anos (sem variantes _pncp).
*/

{{ config(
    materialized='incremental',
    incremental_strategy='append',
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
    * EXCLUDE (data_atualizacao),
    CAST(data_atualizacao AS DATE) AS data_atualizacao
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