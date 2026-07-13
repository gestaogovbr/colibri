{{ config(
    materialized='table',
    database='lake',
    tags=['staging', 'margem_preferencia']
) }}

-- CICS
SELECT id, vigencia_inicio::DATE AS vigencia_inicio
FROM read_csv('s3://{{ var("bucket_lake") }}/margem_preferencia/CICS/resolucoes_CICS.csv', delim=';', auto_detect=true)

UNION ALL

-- CIIA-PAC
SELECT
    trim(id)                                AS id,
    strptime(trim(data), '%d/%m/%Y')::DATE  AS vigencia_inicio
FROM read_csv(
    's3://{{ var("bucket_lake") }}/margem_preferencia/CIIA-PAC/resolucoes_CIIA-PAC.csv',
    delim=';', header=false, all_varchar=true,
    names=['id', 'tipo', 'nome', 'data']
)
WHERE trim(id) != ''
