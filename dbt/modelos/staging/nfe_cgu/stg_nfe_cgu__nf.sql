{{ config(
    materialized='incremental',
    incremental_strategy='append',
    database='lake',
    tags=['staging', 'nfe_cgu']
) }}

WITH raw_data AS (
    SELECT * FROM read_csv(
        '../dados/nfe_cgu/nf/*.csv',
        sep = ';',
        header = true,
        all_varchar = true,
        union_by_name = true,
        filename = true
    )
)

SELECT
    * EXCLUDE (filename),
    regexp_extract(filename, '(\d{4}-\d{2})\.csv$', 1) AS periodo,
    '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' AS _dbt_loaded_at
FROM raw_data

{% if is_incremental() %}
WHERE regexp_extract(filename, '(\d{4}-\d{2})\.csv$', 1) IN (
    SELECT periodo
    FROM read_csv(
        '../dados/alteracoes/nfe_cgu_alteracoes.csv',
        header = true,
        columns = {'tabela': 'VARCHAR', 'periodo': 'VARCHAR'}
    )
    WHERE tabela = 'nf'
)
{% endif %}
