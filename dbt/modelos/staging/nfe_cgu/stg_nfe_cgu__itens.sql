{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='fail',
    database='lake',
    tags=['staging', 'nfe_cgu']
) }}

WITH raw_data AS (
  {{ nfe_cgu_source('itens') }}
)

SELECT
    -- A fonte publica o NCM sem o zero à esquerda (0403.20.00 vem como 4032000).
    -- Com 7 dígitos, o zero é recolocado; o resto (8 dígitos, -1 de serviço, lixo) fica como veio (#98).
    * REPLACE (
        CASE
            WHEN length("CÓDIGO NCM/SH") = 7 THEN '0' || "CÓDIGO NCM/SH"
            ELSE "CÓDIGO NCM/SH"
        END AS "CÓDIGO NCM/SH"
    ),
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
    WHERE tabela = 'itens'
)
{% endif %}
