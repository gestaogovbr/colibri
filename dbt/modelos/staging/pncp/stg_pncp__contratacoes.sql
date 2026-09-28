/*
Modelo Staging: cabeçalhos de contratações lidos da API pública do PNCP.

Complementa o `stg_pncp_comprasgov__compras`, que vem do dump do Compras.gov e só
cobre o que passa pelo sistema federal. Aqui entra o PNCP inteiro — inclusive o
município que publica por sistema próprio. As duas fontes convivem: a chave
`numero_controle_pncp` permite cruzá-las e medir a diferença de cobertura.
*/

{{ config(
    materialized='incremental',
    incremental_strategy='append',
    unique_key=['eixo', 'modalidade_arquivo', 'periodo'],
    database='lake',
    contract={'enforced': false},
    tags=['staging', 'pncp', 'api']
) }}

WITH raw_data AS (
  {{ pncp_api_source() }}
),

bronze AS (
    SELECT
        *,
        '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' AS _dbt_loaded_at
    FROM raw_data
)

SELECT
    numeroControlePNCP                              AS numero_controle_pncp,
    CAST(anoCompra AS INTEGER)                      AS ano_compra,
    CAST(sequencialCompra AS INTEGER)               AS sequencial_compra,
    numeroCompra                                    AS numero_compra,
    processo,
    CAST(modalidadeId AS INTEGER)                   AS modalidade_id,
    modalidadeNome                                  AS modalidade_nome,
    CAST(modoDisputaId AS INTEGER)                  AS modo_disputa_id,
    modoDisputaNome                                 AS modo_disputa_nome,
    CAST(situacaoCompraId AS INTEGER)               AS situacao_compra_id,
    situacaoCompraNome                              AS situacao_compra_nome,
    objetoCompra                                    AS objeto_compra,
    informacaoComplementar                          AS informacao_complementar,
    CAST(valorTotalEstimado AS DOUBLE)              AS valor_total_estimado,
    CAST(valorTotalHomologado AS DOUBLE)            AS valor_total_homologado,
    srp,
    CAST(dataInclusao AS TIMESTAMP)                 AS data_inclusao,
    CAST(dataPublicacaoPncp AS TIMESTAMP)           AS data_publicacao_pncp,
    CAST(dataAtualizacao AS TIMESTAMP)              AS data_atualizacao,
    CAST(dataAberturaProposta AS TIMESTAMP)         AS data_abertura_proposta,
    CAST(dataEncerramentoProposta AS TIMESTAMP)     AS data_encerramento_proposta,
    amparoLegal_codigo                              AS amparo_legal_codigo,
    amparoLegal_nome                                AS amparo_legal_nome,
    orgaoEntidade_cnpj                              AS orgao_entidade_cnpj,
    orgaoEntidade_razaoSocial                       AS orgao_entidade_razao_social,
    orgaoEntidade_esferaId                          AS orgao_entidade_esfera_id,
    orgaoEntidade_poderId                           AS orgao_entidade_poder_id,
    unidadeOrgao_codigoUnidade                      AS unidade_orgao_codigo,
    unidadeOrgao_nomeUnidade                        AS unidade_orgao_nome,
    unidadeOrgao_municipioNome                      AS unidade_orgao_municipio_nome,
    unidadeOrgao_codigoIbge                         AS unidade_orgao_codigo_ibge,
    unidadeOrgao_ufSigla                            AS unidade_orgao_uf_sigla,
    unidadeOrgao_ufNome                             AS unidade_orgao_uf_nome,
    linkSistemaOrigem                               AS link_sistema_origem,
    fontesOrcamentarias                             AS fontes_orcamentarias_json,
    eixo,
    modalidade_arquivo,
    periodo,
    _dbt_loaded_at
FROM bronze

{# Em runs incrementais, insere só os arquivos que o extract gravou nesta execução #}
{% if is_incremental() %}
WHERE (eixo, modalidade_arquivo, periodo) IN (
    SELECT eixo, modalidade, periodo
    FROM read_csv(
        '../dados/alteracoes/pncp_alteracoes.csv',
        header = true,
        columns = {'modalidade': 'INTEGER', 'eixo': 'VARCHAR', 'periodo': 'VARCHAR'}
    )
)
{% endif %}
