{{ config(
    materialized='table',
    database='lake',
    tags=['staging', 'ncm']
) }}

SELECT * FROM read_csv_auto('../dados/dim_margem_ncm_utf8.csv', header = true)
