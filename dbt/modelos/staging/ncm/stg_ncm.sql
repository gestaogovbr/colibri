{{ config(
    materialized='incremental',
    incremental_strategy='append',
    database='lake',
    tags=['staging', 'ncm']
) }}

WITH raw_data AS (
    SELECT * FROM read_csv_auto(
        '../dados/ncm/ncm_silver_*.csv',
        filename = true
    )
)

SELECT
    Codigo                  AS codigo,
    Descricao               AS descricao,
    Nivel                   AS nivel,
    Caminho                 AS caminho,
    Capitulo_Codigo         AS capitulo_codigo,
    Capitulo_Descricao      AS capitulo_descricao,
    Posicao_Codigo          AS posicao_codigo,
    Posicao_Descricao       AS posicao_descricao,
    Subposicao_1_Codigo     AS subposicao_1_codigo,
    Subposicao_1_Descricao  AS subposicao_1_descricao,
    Subposicao_2_Codigo     AS subposicao_2_codigo,
    Subposicao_2_Descricao  AS subposicao_2_descricao,
    Item_Codigo             AS item_codigo,
    Item_Descricao          AS item_descricao,
    Subitem_Codigo          AS subitem_codigo,
    Subitem_Descricao       AS subitem_descricao,
    filename                AS _source_file,
    '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' AS _dbt_loaded_at
FROM raw_data

{# Em runs incrementais, insere só o arquivo gerado pelo extract nesta execução.
   O basename em ncm_alteracoes.csv é comparado com CONTAINS(filename, ...) para
   evitar problemas de separador de path entre Python e DuckDB no Windows. #}
{% if is_incremental() %}
WHERE EXISTS (
    SELECT 1
    FROM read_csv(
        '../dados/ncm_alteracoes.csv',
        header = true,
        columns = {'arquivo_csv': 'VARCHAR'}
    )
    WHERE CONTAINS(raw_data.filename, arquivo_csv)
)
{% endif %}
