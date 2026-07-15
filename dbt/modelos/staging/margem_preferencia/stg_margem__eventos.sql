{{ config(
    materialized='table',
    database='lake',
    tags=['staging', 'margem_preferencia']
) }}

{% set resolucoes_cics = ['cics_1', 'cics_3', 'cics_4', 'cics_7', 'cics_8', 'cics_9'] %}

{% set resolucoes_ciiapac = [
    {'id': 'ciiapac_1', 'aba': 'res_ciia_pac_1'},
    {'id': 'ciiapac_3', 'aba': 'res_ciia_pac_3'},
    {'id': 'ciiapac_4', 'aba': 'res_ciia_pac_4'},
    {'id': 'ciiapac_5', 'aba': 'res_ciia_pac_5'},
] %}

-- cada resolução CICS desabilita 100% da anterior na lista (padrão observado no histórico)
WITH base AS (

    {% for resolucao in resolucoes_cics %}
        {% set anterior = resolucoes_cics[loop.index0 - 1] if not loop.first else none %}
        {{ margem_eventos_cics(resolucao, anterior) }}
        {{ "UNION ALL" if not loop.last }}
    {% endfor %}

    UNION ALL

    {% for r in resolucoes_ciiapac %}
        {{ margem_eventos_ciiapac(r.id, r.aba) }}
        {{ "UNION ALL" if not loop.last }}
    {% endfor %}

),

eventos AS (
    SELECT
        base.*,
        NULL::DECIMAL(3,2)               AS margem_sustentabilidade_pct,
        r.vigencia_inicio::DATE          AS data_evento
    FROM base
    JOIN {{ ref('stg_margem__resolucoes') }} AS r ON r.id = base.resolucao
),

enumerated AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY prefixo_ncm, data_evento, tipo_evento_margem
        ORDER BY (margem_normal_pct + COALESCE(margem_adicional_pct, 0) + COALESCE(margem_sustentabilidade_pct, 0)) DESC
    ) AS rn
    FROM eventos
),

dedup AS (
    SELECT * EXCLUDE (rn)
    FROM enumerated
    WHERE rn = 1
)

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
    grupo_de_produtos
FROM dedup
ORDER BY data_evento, tipo_evento_margem, prefixo_ncm
