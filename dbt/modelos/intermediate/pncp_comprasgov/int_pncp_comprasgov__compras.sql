{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['cod_compra', 'data_atualizacao'],
    database='lake',
    tags=['intermediate', 'pncp', 'comprasgov']
) }}

{# Em runs incrementais, recalcula o histórico SCD2 só dos cod_compra que tiveram
   algum período alterado nesta execução (arquivo gerado por extract.py). Como
   dedup/scd2 são PARTITION BY cod_compra, as linhas desses cod_compra bastam
   pra recalcular o histórico inteiro deles corretamente. #}
WITH

{% if is_incremental() %}
cod_compra_afetados AS (
    SELECT DISTINCT cod_compra
    FROM {{ ref('stg_pncp_comprasgov__compras') }}
    WHERE (granularidade, periodo) IN (
        SELECT granularidade, periodo
        FROM read_csv(
            '../dados/pncp_comprasgov_alteracoes.csv',
            header = true,
            columns = {'view': 'VARCHAR', 'granularidade': 'VARCHAR', 'periodo': 'VARCHAR'}
        )
        WHERE view = 'VW_FT_PNCP_COMPRA'
    )
),
{% endif %}

staging AS (
    SELECT * FROM {{ ref('stg_pncp_comprasgov__compras') }}
    {% if is_incremental() %}
    WHERE cod_compra IN (SELECT cod_compra FROM cod_compra_afetados)
    {% endif %}
),

-- Por (cod_compra, data_atualizacao): escolhe a linha com mais colunas preenchidas
dedup AS (
    SELECT * EXCLUDE (_rn)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY cod_compra, data_atualizacao
                ORDER BY {{ contar_colunas_preenchidas(
                    ref('stg_pncp_comprasgov__compras'),
                    excluir=['cod_compra', 'data_atualizacao', 'granularidade', 'periodo', '_dbt_loaded_at']
                ) }} DESC
            ) AS _rn
        FROM staging
    )
    WHERE _rn = 1
),

scd2 AS (
    SELECT
        * EXCLUDE (granularidade, periodo),
        data_atualizacao                              AS valido_de,
        LEAD(data_atualizacao) OVER (
            PARTITION BY cod_compra
            ORDER BY data_atualizacao
        ) - 1                                        AS valido_ate,
        LEAD(data_atualizacao) OVER (
            PARTITION BY cod_compra
            ORDER BY data_atualizacao
        ) IS NULL                                     AS is_current
    FROM dedup
)

SELECT * FROM scd2
