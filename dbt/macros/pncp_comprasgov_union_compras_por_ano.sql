{% macro pncp_comprasgov_union_compras_por_ano() %}
  SELECT
    * EXCLUDE (filename),
    regexp_extract(filename, 'comprasGOV-(\w+)-VW_FT_PNCP_COMPRA', 1) AS granularidade,
    regexp_extract(filename, 'VW_FT_PNCP_COMPRA-(.+)\.csv$', 1) AS periodo
  FROM read_csv_auto(
    [
      '{{ var("raiz_dados") }}/pncp_comprasgov_diario/*/*/*/comprasGOV-diario-VW_FT_PNCP_COMPRA-*.csv',
      '{{ var("raiz_dados") }}/pncp_comprasgov_mensal/*/*/comprasGOV-mensal-VW_FT_PNCP_COMPRA-*.csv',
      '{{ var("raiz_dados") }}/pncp_comprasgov_anual/*/comprasGOV-anual-VW_FT_PNCP_COMPRA-*.csv'
    ],
    union_by_name = true,
    filename = true
  )
{% endmacro %}
