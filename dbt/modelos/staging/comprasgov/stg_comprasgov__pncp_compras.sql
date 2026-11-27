{{ config(
    materialized='view',
    database='lake',
    tags=['staging', 'pncp', 'comprasgov']
) }}

WITH raw_data AS (
    {{ comprasgov_source ('compras') }}
)

SELECT
    * EXCLUDE (
        modalidade_id_pncp,
        modo_disputa_id_pncp,
        data_atualizacao_pncp,
        ano_compra_pncp,
        sequencial_compra_pncp,
        data_inclusao_pncp,
        amparo_legal_codigo_pncp,
        data_abertura_proposta_pncp,
        data_encerramento_proposta_pncp,
        situacao_compra_id_pncp,
        situacao_compra_nome_pncp,
        tipo_instrumento_convocatorio_codigo_pncp,
        modo_disputa_nome_pncp
    ) REPLACE (
        TRY_CAST(valor_total_estimado AS DECIMAL(28,8))
            AS valor_total_estimado
    ),

    modalidade_id_pncp AS modalidade_id,
    modo_disputa_id_pncp AS modo_disputa_id,
    TRY_CAST(data_atualizacao_pncp AS TIMESTAMP) AS data_atualizacao,
    ano_compra_pncp AS ano_compra,
    sequencial_compra_pncp AS sequencial_compra,
    data_inclusao_pncp AS data_inclusao,
    amparo_legal_codigo_pncp AS amparo_legal_codigo,
    data_abertura_proposta_pncp AS data_abertura_proposta,
    data_encerramento_proposta_pncp AS data_encerramento_proposta,
    situacao_compra_id_pncp AS situacao_compra_id,
    situacao_compra_nome_pncp AS situacao_compra_nome,
    tipo_instrumento_convocatorio_codigo_pncp AS tipo_instrumento_convocatorio_codigo,
    modo_disputa_nome_pncp AS modo_disputa_nome

FROM raw_data