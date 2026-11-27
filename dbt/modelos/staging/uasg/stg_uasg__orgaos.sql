/*
Modelo Staging: snapshots do cadastro de órgãos (Compras.gov.br, /modulo-uasg/2_consultarOrgao).
Cada CSV gerado por ingestion/uasg/extract.py é um snapshot completo (ativos e inativos);
o modelo acumula os snapshots (append) e o histórico de alterações é montado em int_uasg__orgaos.
*/

{{ config(
    materialized='incremental',
    incremental_strategy='append',
    database='lake',
    tags=['staging', 'uasg']
) }}

WITH raw_data AS (
    SELECT * FROM read_csv(
        '../dados/uasg/orgaos_*.csv',
        header = true,
        filename = true,
        all_varchar = true
    )
)

SELECT
    codigoOrgao                                 AS codigo_orgao,
    nomeOrgao                                   AS nome_orgao,
    nomeMnemonicoOrgao                          AS nome_mnemonico_orgao,
    cnpjCpfOrgao                                AS cnpj_cpf_orgao,
    codigoOrgaoVinculado                        AS codigo_orgao_vinculado,
    cnpjCpfOrgaoVinculado                       AS cnpj_cpf_orgao_vinculado,
    nomeOrgaoVinculado                          AS nome_orgao_vinculado,
    codigoOrgaoSuperior                         AS codigo_orgao_superior,
    cnpjCpfOrgaoSuperior                        AS cnpj_cpf_orgao_superior,
    nomeOrgaoSuperior                           AS nome_orgao_superior,
    codigoTipoAdministracao                     AS codigo_tipo_administracao,
    nomeTipoAdministracao                       AS nome_tipo_administracao,
    poder                                       AS poder,
    esfera                                      AS esfera,
    CAST(usoSisg AS BOOLEAN)                    AS uso_sisg,
    CAST(statusOrgao AS BOOLEAN)                AS status_orgao,
    CAST(dataHoraMovimento AS TIMESTAMP)        AS data_hora_movimento,
    CAST(extraido_em AS TIMESTAMP)              AS extraido_em,
    filename                                    AS _source_file,
    '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' AS _dbt_loaded_at
FROM raw_data

{% if is_incremental() %}
WHERE EXISTS (
    SELECT 1
    FROM read_csv(
        '../dados/alteracoes/uasg_alteracoes.csv',
        header = true,
        columns = {'entidade': 'VARCHAR', 'arquivo_csv': 'VARCHAR'}
    )
    WHERE entidade = 'orgaos' AND CONTAINS(raw_data.filename, arquivo_csv)
)
{% endif %}
