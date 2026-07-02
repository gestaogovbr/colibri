{% macro pncp_comprasgov_source(view) %}
  SELECT
    * EXCLUDE (filename),
    regexp_extract(filename, 'comprasGOV-(\w+)-{{ view }}', 1) AS granularidade,
    regexp_extract(filename, '{{ view }}-(.+)\.parquet$', 1) AS periodo
  FROM read_parquet(
    [
      's3://{{ var("bucket_lake") }}/pncp_comprasgov_diario/*/*/*/comprasGOV-diario-{{ view }}-*.parquet',
      's3://{{ var("bucket_lake") }}/pncp_comprasgov_mensal/*/*/comprasGOV-mensal-{{ view }}-*.parquet',
      's3://{{ var("bucket_lake") }}/pncp_comprasgov_anual/*/comprasGOV-anual-{{ view }}-*.parquet'
    ],
    union_by_name = true,
    filename = true
  )
{% endmacro %}
