{{ config(
    materialized='incremental',
    incremental_strategy='append',
    database='lake',
    tags=['staging', 'nfe_cgu']
) }}

WITH raw_data AS (
  {{ nfe_cgu_source('Evento\.parquet$') }}
)

SELECT
    *,
    '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' AS _dbt_loaded_at
FROM raw_data

{% if is_incremental() %}
WHERE periodo IN (
    SELECT periodo
    FROM read_csv(
        '../dados/alteracoes/nfe_cgu_alteracoes.csv',
        header = true,
        columns = {'tabela': 'VARCHAR', 'periodo': 'VARCHAR'}
    )
    WHERE tabela = 'eventos'
)
{% endif %}
