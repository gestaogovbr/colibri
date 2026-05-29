/*
Modelo Bronze: Consolidação de todas as compras públicas
Empilha todos os arquivos PNCP_ITEM_RESULTADO de diferentes anos
*/

{{ config(
    materialized='view',
    tags=['bronze', 'pncp', 'comprasgov']
) }}

WITH raw_data AS (
  {{ pncp_comprasgov_union_compras_por_ano(2021, 2025, 'resultados') }}
  -- Alteração de schema em 2025.
  -- UNION BY NAME utilizado na macro para contornar o problema.
)

SELECT 
    *,
    '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' AS _dbt_loaded_at
FROM raw_data