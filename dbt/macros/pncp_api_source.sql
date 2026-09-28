{# Lê os parquets de cabeçalho vindos da API pública do PNCP (ingestion/pncp/extract.py).
   O nome do arquivo carrega eixo, modalidade e período: pncp-{eixo}-{modalidade}-{periodo}.parquet #}
{% macro pncp_api_source() %}
  SELECT
    * EXCLUDE (filename),
    regexp_extract(filename, 'pncp-(\w+)-\d+-[\d-]+\.parquet$', 1)  AS eixo,
    CAST(regexp_extract(filename, 'pncp-\w+-(\d+)-[\d-]+\.parquet$', 1) AS INTEGER) AS modalidade_arquivo,
    regexp_extract(filename, 'pncp-\w+-\d+-([\d-]+)\.parquet$', 1)  AS periodo
  FROM read_parquet(
    's3://{{ var("bucket_lake") }}/pncp/pncp-*.parquet',
    union_by_name = true,
    filename = true
  )
{% endmacro %}
