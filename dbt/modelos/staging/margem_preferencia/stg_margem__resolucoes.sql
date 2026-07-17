{{ config(
    materialized='table',
    database='lake',
    tags=['staging', 'margem_preferencia']
) }}

{% set path = 's3://' ~ var('bucket_arquivos') ~ '/raw/resolucoes_cics_e_ciia_pac/' ~ var('margem_arquivo_resolucoes') %}

SELECT trim(id) AS id, vigencia_inicio
FROM read_xlsx('{{ path }}', sheet='resolucoes_cics')
WHERE trim(id) != ''

UNION ALL

SELECT trim(id) AS id, vigencia_inicio
FROM read_xlsx('{{ path }}', sheet='resolucoes_ciia_pac')
WHERE trim(id) != ''
