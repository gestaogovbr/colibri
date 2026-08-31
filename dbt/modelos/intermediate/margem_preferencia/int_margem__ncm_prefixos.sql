{{ config(
    materialized='table',
    database='lake',
    tags=['intermediate', 'margem_preferencia'],
    contract={'enforced': true}
) }}

WITH eventos AS (
    SELECT
        e.prefixo_ncm,
        e.resolucao,
        e.tipo_margem,
        CAST(r.vigencia_inicio AS DATE) AS data_evento,
        e.tipo_evento_margem,
        e.margem_normal_comprovante,
        e.margem_normal_pct,
        e.margem_adicional_comprovante,
        e.margem_adicional_pct,
        NULL::VARCHAR                              AS margem_sustentabilidade_comprovante,
        NULL::DECIMAL(3,2)                         AS margem_sustentabilidade_pct,
        e.grupo_de_produtos,
        e.comentario
    FROM {{ ref('stg_margem__eventos') }} e
    JOIN {{ ref('stg_margem__resolucoes') }} r
        ON e.resolucao = r.id
),

eventos_ordenados AS (
    SELECT
        *,
        LEAD(data_evento) OVER (
            PARTITION BY starts_with(resolucao, 'cics'), prefixo_ncm, tipo_margem
            -- desabilita antes de habilita no mesmo dia: evita que o LEAD do habilita aponte pro desabilita do mesmo dia
            ORDER BY data_evento, tipo_evento_margem ASC
        ) AS proxima_data_evento
    FROM eventos
),

intervalos AS (
    SELECT
        prefixo_ncm,
        resolucao,
        tipo_margem,
        data_evento                    AS data_inicio,
        -- CAST necessário: DATE - INTERVAL retorna TIMESTAMP no DuckDB (issue #66)
        CAST(proxima_data_evento - INTERVAL 1 DAY AS DATE) AS data_fim,
        margem_normal_comprovante,
        margem_normal_pct,
        margem_adicional_comprovante,
        margem_adicional_pct,
        margem_sustentabilidade_comprovante,
        margem_sustentabilidade_pct,
        grupo_de_produtos,
        comentario
    FROM eventos_ordenados
    WHERE tipo_evento_margem = 'habilita'
),

numerados AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY starts_with(resolucao, 'cics'), prefixo_ncm
            ORDER BY data_inicio DESC
        ) AS rn
    FROM intervalos
),

marcadores AS (
    SELECT * EXCLUDE (rn),
        rn = 1                    AS ultima,
        rn = 1 AND data_fim IS NULL AS ativa
    FROM numerados
),

enumerated AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY starts_with(resolucao, 'cics'), prefixo_ncm, data_inicio
        ORDER BY (margem_normal_pct + margem_adicional_pct + margem_sustentabilidade_pct) DESC
    ) AS rn
    FROM marcadores
),

dedup AS (
    SELECT * EXCLUDE(rn)
    FROM enumerated
    WHERE rn = 1
)

SELECT * FROM dedup
