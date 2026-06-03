{% macro pncp_comprasgov_union_compras_por_ano() %}
  SELECT *
  FROM read_csv_auto(
    '{{ var("raiz_dados") }}/pncp_comprasgov_diario/*/*/*/comprasGOV-diario-VW_FT_PNCP_COMPRA-*.csv',
    union_by_name = true
  )
{% endmacro %}
