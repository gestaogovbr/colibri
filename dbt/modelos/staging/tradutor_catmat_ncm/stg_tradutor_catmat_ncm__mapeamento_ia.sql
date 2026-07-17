{{ config(
    materialized='table',
    database='lake',
    tags=['staging', 'tradutor_catmat_ncm']
) }}

{% set path = 's3://' ~ var('bucket_arquivos') ~ '/raw/Catálogo de Materiais - parcial.xlsx' %}

SELECT
        trim("Código do Grupo")                       AS codigo_do_grupo,
        trim("Nome do Grupo")                         AS nome_do_grupo,
        trim("Código da Classe")                      AS codigo_da_classe,
        trim("Nome da Classe")                        AS nome_da_classe,
        trim("Código do PDM")                         AS codigo_do_pdm,
        trim("Nome do PDM")                           AS nome_do_pdm,
        trim("Código do Item")                        AS codigo_do_item,
        trim("Descrição do Item")                     AS descricao_do_item,
        trim("Código NCM")                            AS codigo_ncm,
        trim("Capítulo NCM Sugerido")                 AS capitulo_ncm_sugerido,
        trim("Descrição do Capítulo Sugerido")        AS descricao_do_capitulo_sugerido,
        trim("Posição NCM Sugerida")                  AS posicao_ncm_sugerida,
        trim("Descrição da Posição Sugerida")         AS descricao_da_posicao_sugerida,
        trim("NCM Sugerido (top 5)")                  AS ncm_sugerido_top_5,
        trim("Descrição do NCM Sugerido (top 5)")     AS descricao_do_ncm_sugerido_top_5
    FROM read_xlsx('{{ path }}', all_varchar=true)
    WHERE trim("Código do Item") IS NOT NULL AND trim("Código do Item") != ''