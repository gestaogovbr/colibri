{{ config(
    materialized='table',
    database='lake',
    tags=['staging', 'margem_preferencia']
) }}

{% set path = 's3://' ~ var('bucket_arquivos') ~ '/raw/resolucoes_cics_e_ciia_pac/' ~ var('margem_arquivo_resolucoes') %}

{% for aba in ['resolucoes_cics', 'resolucoes_ciia_pac'] %}

SELECT
    trim(id) AS id,
    trim(nome) AS nome,
    CAST(data AS DATE) AS data,
    CAST(publicacao AS DATE) AS publicacao,
    CAST(vacatio_legis_dias AS INTEGER) AS vacatio_legis_dias,
    CAST(vigencia_inicio AS DATE) AS vigencia_inicio,
    trim(sumario) AS sumario,
    trim(url) AS url

FROM read_xlsx('{{ path }}', sheet='{{ aba }}')
WHERE trim(id) != ''

{% if not loop.last %}
UNION ALL
{% endif %}

{% endfor %}
