{#
    Override do macro do adapter dbt-duckdb: o original filtra duckdb_tables()/duckdb_views()
    por t.database_name = '{{ database }}', mas esse `database` sempre resolve pro banco
    primário da conexão (lakehouse_prod/lakehouse_dev), nunca pro catálogo DuckLake anexado
    (lake), onde os models de fato moram (config `database='lake'`). Resultado: a query roda
    sem erro mas nunca bate nenhuma linha, e o catalog.json do `dbt docs generate` fica vazio.
    Aqui filtramos por schema, não por database, o que cobre o caso real do projeto (um único
    catálogo `lake` anexado).
#}
{% macro duckdb__get_catalog(information_schema, schemas) -%}
  {%- call statement('catalog', fetch_result=True) -%}
    with relations AS (
      select
        t.table_name
        , t.database_name
        , t.schema_name
        , 'BASE TABLE' as table_type
        , t.comment as table_comment
      from duckdb_tables() t
      UNION ALL
      SELECT v.view_name as table_name
      , v.database_name
      , v.schema_name
      , 'VIEW' as table_type
      , v.comment as table_comment
      from duckdb_views() v
    )
    select
        r.database_name as table_database,
        r.schema_name as table_schema,
        r.table_name,
        r.table_type,
        r.table_comment,
        c.column_name,
        c.column_index as column_index,
        c.data_type as column_type,
        c.comment as column_comment,
        NULL as table_owner
    FROM relations r JOIN duckdb_columns() c ON r.schema_name = c.schema_name AND r.table_name = c.table_name
    WHERE (
        {%- for schema in schemas -%}
          upper(r.schema_name) = upper('{{ schema }}'){%- if not loop.last %} or {% endif -%}
        {%- endfor -%}
    )
    ORDER BY
        r.schema_name,
        r.table_name,
        c.column_index
  {%- endcall -%}
  {{ return(load_result('catalog').table) }}
{%- endmacro %}
