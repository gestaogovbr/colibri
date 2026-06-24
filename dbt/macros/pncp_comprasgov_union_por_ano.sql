{% macro pncp_comprasgov_union_por_ano(view) %}
  SELECT
    * EXCLUDE (filename),
    regexp_extract(filename, 'comprasGOV-(\w+)-{{ view }}', 1) AS granularidade,
    regexp_extract(filename, '{{ view }}-(.+)\.csv$', 1) AS periodo
  FROM read_csv_auto(
    [
      '../dados/pncp_comprasgov_diario/*/*/*/comprasGOV-diario-{{ view }}-*.csv',
      '../dados/pncp_comprasgov_mensal/*/*/comprasGOV-mensal-{{ view }}-*.csv',
      '../dados/pncp_comprasgov_anual/*/comprasGOV-anual-{{ view }}-*.csv'
    ],
    union_by_name = true,
    filename = true
  )
{% endmacro %}