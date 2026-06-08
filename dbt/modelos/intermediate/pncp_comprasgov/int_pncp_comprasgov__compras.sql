{{ config(
    materialized='table',
    database='lake',
    tags=['intermediate', 'pncp', 'comprasgov']
) }}

WITH bronze AS (
    SELECT * FROM {{ ref('stg_pncp_comprasgov__compras') }}
),

harmonizado AS (
    SELECT
        cod_compra,
        id_compra,
        numero_controle_PNCP,
        numero_compra,
        processo,
        granularidade,
        periodo,

        -- Colunas com variante _pncp: COALESCE prioriza a versão não-pncp (mais completa no diário)
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
        COALESCE(modo_disputa_nome,                    modo_disputa_nome_pncp)                    AS modo_disputa_nome,

        -- Colunas sem variante _pncp
        ind_atual,
        valor_total_estimado,
        valor_total_homologado,
        orgao_subrogado_cnpj,
        orgao_subrogado_razao_social,
        orgao_subrogado_esfera_id,
        orgao_subrogado_poder_id,
        codigo_orgao,
        unidade_orgao_uf_nome,
        unidade_orgao_codigo_unidade,
        unidade_orgao_nome_unidade,
        unidade_orgao_uf_sigla,
        unidade_orgao_municipio_nome,
        unidade_orgao_codigo_ibge,
        unidade_subrogada_uf_nome,
        unidade_subrogada_codigo_unidade,
        unidade_subrogada_nome_unidade,
        unidade_subrogada_uf_sigla,
        unidade_subrogada_municipio_nome,
        unidade_subrogada_codigo_ibge,
        data_publicacao_pncp,
        codigo_modo_disputa,
        codigo_modalidade,
        srp,
        orgao_entidade_cnpj,
        orgao_entidade_razao_social,
        orgao_entidade_esfera_id,
        orgao_entidade_poder_id,
        amparo_legal_nome,
        amparo_legal_descricao,
        informacao_complementar,
        objeto_compra,
        link_sistema_origem,
        justificativa_presencial,
        existe_resultado,
        modalidade_nome,
        orcamento_sigiloso_codigo,
        orcamento_sigiloso_descricao,
        tipo_instrumento_convocatorio_nome,
        usuario_nome,
        contratacao_excluida,
        _dbt_loaded_at
    FROM bronze
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
                    excluir=['cod_compra', 'data_atualizacao', 'data_atualizacao_pncp',
                             'granularidade', 'periodo', '_dbt_loaded_at']
                ) }} DESC
            ) AS _rn
        FROM harmonizado
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
