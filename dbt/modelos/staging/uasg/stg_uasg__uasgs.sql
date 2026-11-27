/*
Modelo Staging: snapshots do cadastro de UASGs (Compras.gov.br, /modulo-uasg/1_consultarUasg).
Cada CSV gerado por ingestion/uasg/extract.py é um snapshot completo (ativas e inativas);
o modelo acumula os snapshots (append) e o histórico de alterações é montado em int_uasg__uasgs.
*/

{{ config(
    materialized='incremental',
    incremental_strategy='append',
    database='lake',
    tags=['staging', 'uasg']
) }}

WITH raw_data AS (
    SELECT * FROM read_csv(
        '../dados/uasg/uasgs_*.csv',
        header = true,
        filename = true,
        all_varchar = true
    )
)

SELECT
    codigoUasg                                  AS codigo_uasg,
    nomeUasg                                    AS nome_uasg,
    CAST(usoSisg AS BOOLEAN)                    AS uso_sisg,
    CAST(adesaoSiasg AS BOOLEAN)                AS adesao_siasg,
    siglaUf                                     AS sigla_uf,
    codigoMunicipio                             AS codigo_municipio,
    codigoMunicipioIbge                         AS codigo_municipio_ibge,
    nomeMunicipioIbge                           AS nome_municipio_ibge,
    NULLIF(codigoUnidadePolo, '0')              AS codigo_unidade_polo,
    nomeUnidadePolo                             AS nome_unidade_polo,
    NULLIF(codigoUnidadeEspelho, '0')           AS codigo_unidade_espelho,
    nomeUnidadeEspelho                          AS nome_unidade_espelho,
    CAST(uasgCadastradora AS BOOLEAN)           AS uasg_cadastradora,
    NULLIF(cnpjCpfUasg, '0')                    AS cnpj_cpf_uasg,
    codigoOrgao                                 AS codigo_orgao,
    cnpjCpfOrgao                                AS cnpj_cpf_orgao,
    cnpjCpfOrgaoVinculado                       AS cnpj_cpf_orgao_vinculado,
    cnpjCpfOrgaoSuperior                        AS cnpj_cpf_orgao_superior,
    codigoSiorg                                 AS codigo_siorg,
    CAST(statusUasg AS BOOLEAN)                 AS status_uasg,
    CAST(dataImplantacaoSidec AS TIMESTAMPTZ)   AS data_implantacao_sidec,
    CAST(dataHoraMovimento AS TIMESTAMP)        AS data_hora_movimento,
    CAST(extraido_em AS TIMESTAMP)              AS extraido_em,
    filename                                    AS _source_file,
    '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' AS _dbt_loaded_at
FROM raw_data

{# Em runs incrementais, insere só os snapshots gerados pelo extract nesta execução.
   O basename em uasg_alteracoes.csv é comparado com CONTAINS(filename, ...) para
   evitar problemas de separador de path entre Python e DuckDB no Windows. #}
{% if is_incremental() %}
WHERE EXISTS (
    SELECT 1
    FROM read_csv(
        '../dados/alteracoes/uasg_alteracoes.csv',
        header = true,
        columns = {'entidade': 'VARCHAR', 'arquivo_csv': 'VARCHAR'}
    )
    WHERE entidade = 'uasgs' AND CONTAINS(raw_data.filename, arquivo_csv)
)
{% endif %}
