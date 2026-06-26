/*
Modelo Staging: Consolidação de todas as compras públicas
Empilha todos os arquivos PNCP_COMPRA de diferentes anos e harmoniza colunas
renomeadas com sufixo _pncp (alteração de schema em 2025).
*/

{{ config(
    materialized='incremental',
    incremental_strategy='append',
    unique_key=['granularidade', 'periodo'],
    database='lake',
    tags=['staging', 'pncp', 'comprasgov']
) }}

WITH raw_data AS (
  {{ pncp_comprasgov_union_por_ano('VW_FT_PNCP_COMPRA') }}
),

bronze AS (
    SELECT
        *,
        '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' AS _dbt_loaded_at
    FROM raw_data
)

SELECT
    * EXCLUDE (
        modalidade_id, modalidade_id_pncp,
        modo_disputa_id, modo_disputa_id_pncp,
        data_atualizacao, data_atualizacao_pncp,
        ano_compra, ano_compra_pncp,
        sequencial_compra, sequencial_compra_pncp,
        data_inclusao, data_inclusao_pncp,
        amparo_legal_codigo, amparo_legal_codigo_pncp,
        data_abertura_proposta, data_abertura_proposta_pncp,
        data_encerramento_proposta, data_encerramento_proposta_pncp,
        situacao_compra_id, situacao_compra_id_pncp,
        situacao_compra_nome, situacao_compra_nome_pncp,
        tipo_instrumento_convocatorio_codigo, tipo_instrumento_convocatorio_codigo_pncp,
        modo_disputa_nome, modo_disputa_nome_pncp
    ),

    -- Colunas com variante _pncp
    COALESCE(modalidade_id,                        modalidade_id_pncp)                        AS modalidade_id,
    COALESCE(modo_disputa_id,                      modo_disputa_id_pncp)                      AS modo_disputa_id,
    CAST(COALESCE(data_atualizacao, data_atualizacao_pncp) AS DATE)                          AS data_atualizacao,
    COALESCE(ano_compra,                           ano_compra_pncp)                           AS ano_compra,
    COALESCE(sequencial_compra,                    sequencial_compra_pncp)                    AS sequencial_compra,
    COALESCE(data_inclusao,                        data_inclusao_pncp)                        AS data_inclusao,
    COALESCE(amparo_legal_codigo,                  amparo_legal_codigo_pncp)                  AS amparo_legal_codigo,
    COALESCE(data_abertura_proposta,               data_abertura_proposta_pncp)               AS data_abertura_proposta,
    COALESCE(data_encerramento_proposta,           data_encerramento_proposta_pncp)           AS data_encerramento_proposta,
    COALESCE(situacao_compra_id,                   situacao_compra_id_pncp)                   AS situacao_compra_id,
    COALESCE(situacao_compra_nome,                 situacao_compra_nome_pncp)                 AS situacao_compra_nome,
    COALESCE(tipo_instrumento_convocatorio_codigo, tipo_instrumento_convocatorio_codigo_pncp) AS tipo_instrumento_convocatorio_codigo,
    COALESCE(modo_disputa_nome,                    modo_disputa_nome_pncp)                    AS modo_disputa_nome
FROM bronze

{# Em runs incrementais, insere só as linhas dos arquivos que o extract baixou/atualizou nesta
   execução (arquivo gerado por extract.py, que sempre roda antes do dbt no pipeline) #}
{% if is_incremental() %}
WHERE (granularidade, periodo) IN (
    SELECT granularidade, periodo
    FROM read_csv(
        '../dados/alteracoes/pncp_comprasgov_alteracoes.csv',
        header = true,
        columns = {'view': 'VARCHAR', 'granularidade': 'VARCHAR', 'periodo': 'VARCHAR'}
    )
    WHERE view = 'VW_FT_PNCP_COMPRA'
)
{% endif %}
