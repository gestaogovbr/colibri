{{
  config(
    materialized='incremental',
    unique_key=['id_compra', 'id_dt_atualizacao'],
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    tags=['staging', 'pncp', 'databricks']
  )
}}

select *
from {{ source('pncp', 'compras') }}

{% if is_incremental() %}
-- Only load versions newer than what's already staged.
-- The source glob reads all daily Parquet files; this predicate
-- ensures already-loaded versions are skipped on subsequent runs.
where id_dt_atualizacao > (select max(id_dt_atualizacao) from {{ this }})
{% endif %}
