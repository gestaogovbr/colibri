{% macro nfe_cgu_source(tabela) %}
  SELECT
    * EXCLUDE (filename),
    regexp_extract(filename, '(\d{4}-\d{2})\.parquet$', 1) AS periodo
  FROM read_parquet(
    's3://{{ var("bucket_lake") }}/nfe_cgu/{{ tabela }}/*.parquet',
    union_by_name = true,
    filename = true
  )
{% endmacro %}
