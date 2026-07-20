{{ config(
    materialized='table',
    database='lake',
    tags=['intermediate', 'ncm']
) }}

{% set path_pattern = 's3://' ~ var('bucket_lake') ~ '/raw/ncm/*.csv' %}
{% if execute %}
    {% set resultado = run_query("SELECT file FROM glob('" ~ path_pattern ~ "') ORDER BY file DESC LIMIT 1") %}
    {% set path = resultado.columns[0].values()[0] %}
{% else %}
    {% set path = '' %}
{% endif %}

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
    Item_Codigo              AS item_codigo,
    Item_Descricao           AS item_descricao,
    Subitem_Codigo           AS subitem_codigo,
    Subitem_Descricao        AS subitem_descricao
FROM read_csv_auto('{{ path }}')
