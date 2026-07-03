{{ config(
    materialized='table',
    database='lake',
    tags=['staging', 'margem_preferencia']
) }}

-- resolucoes_CICS.csv já vem com id + vigencia_inicio prontos.
-- resolucoes_CIIA-PAC.csv não tem cabeçalho, traz lixo no topo (título do
-- decreto, linha em branco) e resoluções que não tratam de margem (ex.:
-- regimento interno) — essas ficam de fora por não terem uma linha
-- correspondente em CIIA-PAC/res_*.csv. Também não há vacatio legis
-- informado: a vigência é a própria data de publicação da resolução.

-- CICS
SELECT id, vigencia_inicio::DATE AS vigencia_inicio
FROM read_csv('../dados/margem_preferencia/CICS/resolucoes_CICS.csv', delim=';', auto_detect=true)

UNION ALL

-- CIIA-PAC
SELECT
    trim(id)                                AS id,
    strptime(trim(data), '%d/%m/%Y')::DATE  AS vigencia_inicio
FROM read_csv(
    '../dados/margem_preferencia/CIIA-PAC/resolucoes_CIIA-PAC.csv',
    delim=';', header=false, all_varchar=true,
    names=['id', 'tipo', 'nome', 'data']
)
WHERE trim(id) != ''
