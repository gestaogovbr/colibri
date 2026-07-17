{{ config(
    materialized='view',
    database='lake',
    
    tags=['marts', 'tradutor_catmat_ncm']
) }}

WITH fonte AS (
    SELECT
        catmat,
        ncm                                  AS ncm_humano,
        ncm IS NOT NULL                      AS usa_humano,
        ncm_capitulo_sugerido_cod,
        ncm_posicao_sugerida_cod,
        ncm_subitem_sugerido_top_5_cod[1]    AS subitem_ncm_ia
    FROM {{ ref('stg_tradutor_catmat_ncm__mapeamento_ia') }}
)

-- O mapeamento humano sempre prevalece quando existe
SELECT
    catmat,
    ncm_humano,
    CASE
        WHEN usa_humano THEN LEFT(ncm_humano, 2)
        ELSE ncm_capitulo_sugerido_cod
    END AS capitulo_ncm,
    CASE
        WHEN usa_humano AND LENGTH(ncm_humano) >= 4 THEN LEFT(ncm_humano, 4)
        WHEN usa_humano THEN NULL
        ELSE ncm_posicao_sugerida_cod
    END AS posicao_ncm,
    CASE
        WHEN usa_humano AND LENGTH(ncm_humano) = 8 THEN ncm_humano
        WHEN usa_humano THEN NULL
        ELSE subitem_ncm_ia
    END AS subitem_ncm,
    CASE
        WHEN usa_humano THEN 'humano'
        WHEN ncm_capitulo_sugerido_cod IS NOT NULL THEN  'ia'
        ELSE NULL END AS metodo_de_mapeamento
FROM fonte
