{{ config(
    materialized='incremental',
    incremental_strategy='append',
    database='lake',
    tags=['staging', 'pncp_em_numeros']
) }}

WITH raw_data AS (
    SELECT * FROM read_parquet(
        '../dados/pncp_em_numeros/agg_compra_*.parquet',
        filename = true
    )
)

SELECT
    raw_data.* EXCLUDE (filename),
    filename                                             AS _source_file,
    '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}'  AS _dbt_loaded_at
FROM raw_data

{# Em runs incrementais, insere só os arquivos de delta gerados pelo extract
   nesta execução. O basename em pncp_em_numeros_alteracoes.csv. #}
{% if is_incremental() %}
WHERE EXISTS (
    SELECT 1
    FROM read_csv(
        '../dados/alteracoes/pncp_em_numeros_alteracoes.csv',
        header = true,
        columns = {'tabela': 'VARCHAR', 'arquivo': 'VARCHAR'}
    )
    WHERE tabela = 'agg_compra'
      AND CONTAINS(raw_data.filename, arquivo)
)
{% endif %}
