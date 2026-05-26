{% macro pncp_comprasgov_union_compras_por_ano(ano_inicio, ano_fim, tabela) %}
  {% for ano in range(ano_inicio, ano_fim + 1) %}
    SELECT *, {{ ano }} as ano_do_arquivo_de_origem 
    {% if tabela == 'compras' %}
      FROM read_csv_auto('/Users/heitor/repos/colibri/dados/pncp_comprasgov/{{ ano }}/*VW_FT_PNCP_COMPRA?{{ ano }}.csv')
    {% endif %}
    {% if tabela == 'itens' %}
      FROM read_csv_auto('/Users/heitor/repos/colibri/dados/pncp_comprasgov/{{ ano }}/*VW_FT_PNCP_COMPRA_ITEM?{{ ano }}.csv')
    {% endif %}
       {% if tabela == 'resultados' %}
      FROM read_csv_auto('/Users/heitor/repos/colibri/dados/pncp_comprasgov/{{ ano }}/*VW_DM_PNCP_ITEM_RESULTADO?{{ ano }}.csv')
    {% endif %}
    {% if not loop.last %}
      UNION BY NAME
    {% endif %}
  {% endfor %}
{% endmacro %}