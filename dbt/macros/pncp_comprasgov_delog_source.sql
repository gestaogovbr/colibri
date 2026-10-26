{% macro pncp_comprasgov_delog_source(tipo) %}

    {% if var('delog_local', false) %}

        {% set origem = '../dados/pncp_comprasgov_delog/' ~ tipo ~ '/*/*/' ~ tipo ~ '_*.parquet' %}

    {% else %}

        {% set origem = 's3://' ~ var('bucket_lake') ~ '/pncp_comprasgov_delog/' ~ tipo ~ '/*/*/' ~ tipo ~ '_*.parquet' %}

    {% endif %}

    SELECT
        * EXCLUDE (filename),

        'mensal' AS granularidade,

        regexp_replace(
            regexp_extract(
                filename,
                '{{ tipo }}_(\d{4}_\d{2})\.parquet$',
                1
            ),
            '_',
            '-'
        ) AS periodo

    FROM read_parquet(
        '{{ origem }}',
        union_by_name = true,
        filename = true
    )

{% endmacro %}