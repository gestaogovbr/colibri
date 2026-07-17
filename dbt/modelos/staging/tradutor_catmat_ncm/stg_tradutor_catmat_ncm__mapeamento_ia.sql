{{ config(
    materialized='table',
    database='lake',
    contract={'enforced': true},
    tags=['staging', 'tradutor_catmat_ncm']
) }}

{% set path = 's3://' ~ var('bucket_arquivos') ~ '/raw/Catálogo de Materiais - parcial.xlsx' %}

SELECT
        CAST(trim("Código do Grupo") AS INT)          AS grupo_cod,
        trim("Nome do Grupo")                         AS grupo_descr,
        CAST(trim("Código da Classe") AS INT)         AS classe_cod,
        trim("Nome da Classe")                        AS classe_descr,
        CAST(trim("Código do PDM") AS INT)            AS pdm_cod,
        trim("Nome do PDM")                           AS pdm_nome,
        CAST(trim("Código do Item") AS INT)           AS catmat,
        trim("Descrição do Item")                     AS catmat_descr,
        NULLIF(trim("Código NCM"), '-')               AS ncm,
        trim("Capítulo NCM Sugerido")                 AS ncm_capitulo_sugerido_cod,
        trim("Descrição do Capítulo Sugerido")        AS ncm_capitulo_sugerido_descr,
        trim("Posição NCM Sugerida")                  AS ncm_posicao_sugerida_cod,
        trim("Descrição da Posição Sugerida")         AS ncm_posicao_sugerida_descr,
        string_split(trim("NCM Sugerido (top 5)"), ',')                  AS ncm_subitem_sugerido_top_5_cod,
        string_split(trim("Descrição do NCM Sugerido (top 5)"), '|')     AS ncm_subitem_sugerido_top_5_descr
    FROM read_xlsx('{{ path }}', all_varchar=true)
    WHERE trim("Código do Item") IS NOT NULL AND trim("Código do Item") != ''