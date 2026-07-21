{% macro nfe_cgu_source(regex) %}
  SELECT
    * EXCLUDE (filename),
    regexp_extract(filename, '(\d{4}-\d{2})/[^/]+$', 1) AS periodo
  FROM read_parquet(
    's3://{{ var("bucket_lake") }}/raw/nfe_cgu/*/*.parquet',
    union_by_name = true,
    filename = true
  )
  WHERE regexp_matches(filename, '{{ regex }}')
{% endmacro %}
