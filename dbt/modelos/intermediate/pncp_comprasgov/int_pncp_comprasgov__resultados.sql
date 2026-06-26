{{ config(
    materialized='view',
    database='lake',
    tags=['intermediate', 'pncp', 'comprasgov']
) }}

WITH staging AS (
    SELECT * FROM {{ ref('stg_pncp_comprasgov__resultados') }}
),

{# Para evitar que o modelo cresça indefinidamente em runs incrementais, 
   aplicamos uma deduplicação prévia para registros idênticos #}
dedup_append AS (
    SELECT * EXCLUDE (_dbt_loaded_at, granularidade, periodo)
    FROM staging
    GROUP BY ALL
),

{# Remove duplicatas, mantendo somente o registro que tiver mais colunas preenchidas.
   Talvez, isso pode remover correções do passado, que deveriam cair no LEAD #}
dedup AS (
    SELECT * EXCLUDE (_rn)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY id_compra_item, sequencial_resultado, data_atualizacao
                ORDER BY {{ contar_colunas_preenchidas(
                    ref('stg_pncp_comprasgov__resultados'),
                    excluir=['_dbt_loaded_at', 'granularidade', 'periodo']
                ) }} DESC
            ) AS _rn
        FROM dedup_append
    )
    WHERE _rn = 1
),

scd2 AS (
    SELECT
        *,
        data_atualizacao                              AS valido_de,
        LEAD(data_atualizacao) OVER (
            PARTITION BY id_compra_item, sequencial_resultado
            ORDER BY data_atualizacao
        ) - 1                                        AS valido_ate,
        LEAD(data_atualizacao) OVER (
            PARTITION BY id_compra_item, sequencial_resultado
            ORDER BY data_atualizacao
        ) IS NULL                                     AS is_current
    FROM dedup
)

SELECT * FROM scd2