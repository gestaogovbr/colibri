{{ config(
    materialized='view',
    database='lake',
    tags=['marts', 'tradutor_catmat_ncm']
) }}

WITH fonte AS (
    SELECT
        codigo_do_item                                AS catmat,
        NULLIF(codigo_ncm, '-')                       AS ncm_humano,
        capitulo_ncm_sugerido,
        posicao_ncm_sugerida,
        split_part(ncm_sugerido_top_5, ',', 1)        AS subitem_ncm_ia
    FROM {{ ref('stg_tradutor_catmat_ncm__mapeamento_ia') }}
),

-- O mapeamento humano sempre prevalece quando existe
classificado AS (
    SELECT *, ncm_humano IS NOT NULL AS usa_humano
    FROM fonte
)

SELECT
    catmat,
    CASE
        WHEN usa_humano THEN LEFT(ncm_humano, 2)
        ELSE capitulo_ncm_sugerido
    END AS capitulo_ncm,
    CASE
        WHEN usa_humano AND LENGTH(ncm_humano) >= 4 THEN LEFT(ncm_humano, 4)
        WHEN usa_humano THEN NULL
        ELSE posicao_ncm_sugerida
    END AS posicao_ncm,
    CASE
        WHEN usa_humano AND LENGTH(ncm_humano) = 8 THEN ncm_humano
        WHEN usa_humano THEN NULL
        ELSE subitem_ncm_ia
    END AS subitem_ncm,
    CASE WHEN usa_humano THEN 'humano' ELSE 'ia' END AS metodo_de_mapeamento
FROM classificado
