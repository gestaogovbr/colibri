{{ config(
    materialized='table',
    database='lake',
    tags=['staging', 'margem_preferencia']
) }}

{% set resolucoes_cics = ['cics_1', 'cics_3', 'cics_4', 'cics_7', 'cics_8', 'cics_9'] %}
{% set resolucoes_ciiapac = ['ciiapac_1', 'ciiapac_3', 'ciiapac_4', 'ciiapac_5'] %}

-- Monta tabela base UNINDO os eventos de cada resolução (uma p/ resolução: ver macros margem_eventos_cics/margem_eventos_ciiapac)
WITH base AS (

    {% for resolucao in resolucoes_cics %}
        {{ margem_eventos_cics(resolucao) }}
        {{ "UNION ALL" if not loop.last }}
    {% endfor %}

    UNION ALL

    {% for resolucao in resolucoes_ciiapac %}
        {{ margem_eventos_ciiapac(resolucao) }}
        {{ "UNION ALL" if not loop.last }}
    {% endfor %}

),

-- Pega data_evento cruzando com tabela de resoluções, e já deixa coluna de margem_sustentabilidade_pct pronta pra ser preenchida depois
eventos AS (
    SELECT
        base.*,
        NULL::DECIMAL(3,2)               AS margem_sustentabilidade_pct,
        r.vigencia_inicio::DATE          AS data_evento
    FROM base
    JOIN {{ ref('stg_margem__resolucoes') }} AS r ON r.id = base.resolucao
),

-- Particiona por prefixo_ncm + data_evento + tipo_evento_margem e ordena pela maior margem total
enumerated AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY prefixo_ncm, data_evento, tipo_evento_margem
        ORDER BY (margem_normal_pct + COALESCE(margem_adicional_pct, 0) + COALESCE(margem_sustentabilidade_pct, 0)) DESC
    ) AS rn
    FROM eventos
),

-- Pega só as linhas que o row number é 1 (maior margem total) pra evitar duplicidade
dedup AS (
    SELECT * EXCLUDE (rn)
    FROM enumerated
    WHERE rn = 1
)

-- Monta tabela fato final, sem duplicidades e pós cruzamento com resoluções pra pegar data_evento
SELECT
    ROW_NUMBER() OVER (ORDER BY data_evento, tipo_evento_margem, prefixo_ncm)
                                     AS id_evento_margem,
    tipo_evento_margem,
    tipo_margem,
    resolucao,
    data_evento,
    prefixo_ncm,
    margem_normal_comprovante,
    margem_normal_pct,
    margem_adicional_comprovante,
    margem_adicional_pct,
    margem_sustentabilidade_pct,
    grupo_cics
FROM dedup
ORDER BY data_evento, tipo_evento_margem, prefixo_ncm
