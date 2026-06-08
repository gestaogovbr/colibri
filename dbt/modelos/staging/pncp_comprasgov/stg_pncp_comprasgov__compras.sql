/*
Modelo Bronze: Consolidação de todas as compras públicas
Empilha todos os arquivos PNCP_COMPRA de diferentes anos
*/

{{ config(
    materialized='table',
    database='lake',
    tags=['bronze', 'pncp', 'comprasgov']
) }}

WITH raw_data AS (
  {{ pncp_comprasgov_union_compras_por_ano() }}
)

SELECT 
    *,
    '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' AS _dbt_loaded_at
FROM raw_data