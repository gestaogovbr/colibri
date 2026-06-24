{% macro contar_colunas_preenchidas(relation, excluir=[]) %}
  {% set cols = adapter.get_columns_in_relation(relation) %}
  {% set exprs = [] %}
  {% for col in cols %}
    {% set nome = col.name | lower %}
    {% if nome not in excluir | map('lower') | list
       and not nome.endswith('_pncp') %}
      {% do exprs.append(
        "(CASE WHEN " ~ col.name ~ " IS NOT NULL THEN 1 ELSE 0 END)"
      ) %}
    {% endif %}
  {% endfor %}
  ({{ exprs | join(' +\n            ') }})
{% endmacro %}