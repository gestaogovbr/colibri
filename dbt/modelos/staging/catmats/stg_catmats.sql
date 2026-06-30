{{ config(
    materialized='table',
    database='lake',
    tags=['staging', 'catmats']
) }}

SELECT * FROM read_csv_auto('../dados/catmats/catmats_api.csv', header = true)
