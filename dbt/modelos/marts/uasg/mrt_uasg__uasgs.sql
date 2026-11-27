{{ config(
    materialized='view',
    database='lake',
    tags=['marts', 'uasg']
) }}

-- Versão vigente de cada UASG, com os atributos do órgão ao qual pertence (esfera, poder,
-- tipo de administração). uso_sisg é o da UASG; uso_sisg_orgao, o do órgão.

WITH uasgs AS (
    SELECT * EXCLUDE (is_current, valido_ate)
    FROM {{ ref('int_uasg__uasgs') }}
    WHERE is_current
),

orgaos AS (
    SELECT *
    FROM {{ ref('int_uasg__orgaos') }}
    WHERE is_current
)

SELECT
    u.*,
    o.nome_orgao,
    o.nome_mnemonico_orgao,
    o.codigo_orgao_vinculado,
    o.nome_orgao_vinculado,
    o.codigo_orgao_superior,
    o.nome_orgao_superior,
    o.codigo_tipo_administracao,
    o.nome_tipo_administracao,
    o.poder,
    o.esfera,
    o.uso_sisg      AS uso_sisg_orgao,
    o.status_orgao
FROM uasgs u
LEFT JOIN orgaos o USING (codigo_orgao)
