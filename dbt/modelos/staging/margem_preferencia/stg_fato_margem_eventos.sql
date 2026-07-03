{{ config(
    materialized='table',
    database='lake',
    tags=['staging', 'margem_preferencia']
) }}

WITH base AS (

        -- Eventos de habilitação/desabilitação de margem normal (conteudo_nacional)
        SELECT
            'cics_1'                                                       AS resolucao,
            trim(habilita)                                                   AS tipo_evento_margem,
            'conteudo_nacional'::VARCHAR                                     AS tipo_margem,
            trim(ncm)                                                        AS prefixo_ncm,
            trim(regra_de_origem)                                            AS margem_normal_comprovante,
            round(TRY_CAST(trim(margem_normal)         AS DOUBLE) / 100, 4)  AS margem_normal_pct,
            NULL::VARCHAR                                                    AS margem_adicional_comprovante,
            NULL::DECIMAL(3,2)                                               AS margem_adicional_pct,
            trim(grupo_cics)                                                 AS grupo_cics
        FROM read_csv('s3://{{ var("bucket_lake") }}/margem_preferencia/CICS/res_1_CICS.csv', sep=';', all_varchar=true)
        WHERE trim(habilita) IS NOT NULL AND trim(habilita) != ''

        UNION ALL

        -- Eventos de habilitação/desabilitação de margem adicional (tecnologia_nacional)
        SELECT
            'cics_1'                                                       AS resolucao,
            trim(habilita)                                                   AS tipo_evento_margem,
            'tecnologia_nacional'::VARCHAR                                   AS tipo_margem,
            trim(ncm)                                                        AS prefixo_ncm,
            trim(regra_de_origem)                                            AS margem_normal_comprovante,
            round(TRY_CAST(trim(margem_normal)         AS DOUBLE) / 100, 4)  AS margem_normal_pct,
            replace(trim(regra_de_qualificacao), ' ', '_')                   AS margem_adicional_comprovante,
            round(TRY_CAST(trim(margem_adicional)      AS DOUBLE) / 100, 4)  AS margem_adicional_pct,
            trim(grupo_cics)                                                 AS grupo_cics
        FROM read_csv('s3://{{ var("bucket_lake") }}/margem_preferencia/CICS/res_1_CICS.csv', sep=';', all_varchar=true)
        WHERE trim(habilita) IS NOT NULL AND trim(habilita) != ''
            AND trim(margem_adicional) IS NOT NULL AND trim(margem_adicional) != ''

    UNION ALL

        -- Eventos de habilitação/desabilitação de margem normal (conteudo_nacional)
        SELECT
            'cics_3'                                                       AS resolucao,
            trim(habilita)                                                   AS tipo_evento_margem,
            'conteudo_nacional'::VARCHAR                                     AS tipo_margem,
            trim(ncm)                                                        AS prefixo_ncm,
            trim(regra_de_origem)                                            AS margem_normal_comprovante,
            round(TRY_CAST(trim(margem_normal)         AS DOUBLE) / 100, 4)  AS margem_normal_pct,
            NULL::VARCHAR                                                    AS margem_adicional_comprovante,
            NULL::DECIMAL(3,2)                                               AS margem_adicional_pct,
            trim(grupo_cics)                                                 AS grupo_cics
        FROM read_csv('s3://{{ var("bucket_lake") }}/margem_preferencia/CICS/res_3_CICS.csv', sep=';', all_varchar=true)
        WHERE trim(habilita) IS NOT NULL AND trim(habilita) != ''

        UNION ALL

        -- Eventos de habilitação/desabilitação de margem adicional (tecnologia_nacional)
        SELECT
            'cics_3'                                                       AS resolucao,
            trim(habilita)                                                   AS tipo_evento_margem,
            'tecnologia_nacional'::VARCHAR                                   AS tipo_margem,
            trim(ncm)                                                        AS prefixo_ncm,
            trim(regra_de_origem)                                            AS margem_normal_comprovante,
            round(TRY_CAST(trim(margem_normal)         AS DOUBLE) / 100, 4)  AS margem_normal_pct,
            replace(trim(regra_de_qualificacao), ' ', '_')                   AS margem_adicional_comprovante,
            round(TRY_CAST(trim(margem_adicional)      AS DOUBLE) / 100, 4)  AS margem_adicional_pct,
            trim(grupo_cics)                                                 AS grupo_cics
        FROM read_csv('s3://{{ var("bucket_lake") }}/margem_preferencia/CICS/res_3_CICS.csv', sep=';', all_varchar=true)
        WHERE trim(habilita) IS NOT NULL AND trim(habilita) != ''
            AND trim(margem_adicional) IS NOT NULL AND trim(margem_adicional) != ''

    UNION ALL

        -- Eventos de habilitação/desabilitação de margem normal (conteudo_nacional)
        SELECT
            'cics_4'                                                       AS resolucao,
            trim(habilita)                                                   AS tipo_evento_margem,
            'conteudo_nacional'::VARCHAR                                     AS tipo_margem,
            trim(ncm)                                                        AS prefixo_ncm,
            trim(regra_de_origem)                                            AS margem_normal_comprovante,
            round(TRY_CAST(trim(margem_normal)         AS DOUBLE) / 100, 4)  AS margem_normal_pct,
            NULL::VARCHAR                                                    AS margem_adicional_comprovante,
            NULL::DECIMAL(3,2)                                               AS margem_adicional_pct,
            trim(grupo_cics)                                                 AS grupo_cics
        FROM read_csv('s3://{{ var("bucket_lake") }}/margem_preferencia/CICS/res_4_CICS.csv', sep=';', all_varchar=true)
        WHERE trim(habilita) IS NOT NULL AND trim(habilita) != ''

        UNION ALL

        -- Eventos de habilitação/desabilitação de margem adicional (tecnologia_nacional)
        SELECT
            'cics_4'                                                       AS resolucao,
            trim(habilita)                                                   AS tipo_evento_margem,
            'tecnologia_nacional'::VARCHAR                                   AS tipo_margem,
            trim(ncm)                                                        AS prefixo_ncm,
            trim(regra_de_origem)                                            AS margem_normal_comprovante,
            round(TRY_CAST(trim(margem_normal)         AS DOUBLE) / 100, 4)  AS margem_normal_pct,
            replace(trim(regra_de_qualificacao), ' ', '_')                   AS margem_adicional_comprovante,
            round(TRY_CAST(trim(margem_adicional)      AS DOUBLE) / 100, 4)  AS margem_adicional_pct,
            trim(grupo_cics)                                                 AS grupo_cics
        FROM read_csv('s3://{{ var("bucket_lake") }}/margem_preferencia/CICS/res_4_CICS.csv', sep=';', all_varchar=true)
        WHERE trim(habilita) IS NOT NULL AND trim(habilita) != ''
            AND trim(margem_adicional) IS NOT NULL AND trim(margem_adicional) != ''

    UNION ALL

        -- Eventos de habilitação/desabilitação de margem normal (conteudo_nacional)
        SELECT
            'cics_7'                                                       AS resolucao,
            trim(habilita)                                                   AS tipo_evento_margem,
            'conteudo_nacional'::VARCHAR                                     AS tipo_margem,
            trim(ncm)                                                        AS prefixo_ncm,
            trim(regra_de_origem)                                            AS margem_normal_comprovante,
            round(TRY_CAST(trim(margem_normal)         AS DOUBLE) / 100, 4)  AS margem_normal_pct,
            NULL::VARCHAR                                                    AS margem_adicional_comprovante,
            NULL::DECIMAL(3,2)                                               AS margem_adicional_pct,
            trim(grupo_cics)                                                 AS grupo_cics
        FROM read_csv('s3://{{ var("bucket_lake") }}/margem_preferencia/CICS/res_7_CICS.csv', sep=';', all_varchar=true)
        WHERE trim(habilita) IS NOT NULL AND trim(habilita) != ''

        UNION ALL

        -- Eventos de habilitação/desabilitação de margem adicional (tecnologia_nacional)
        SELECT
            'cics_7'                                                       AS resolucao,
            trim(habilita)                                                   AS tipo_evento_margem,
            'tecnologia_nacional'::VARCHAR                                   AS tipo_margem,
            trim(ncm)                                                        AS prefixo_ncm,
            trim(regra_de_origem)                                            AS margem_normal_comprovante,
            round(TRY_CAST(trim(margem_normal)         AS DOUBLE) / 100, 4)  AS margem_normal_pct,
            replace(trim(regra_de_qualificacao), ' ', '_')                   AS margem_adicional_comprovante,
            round(TRY_CAST(trim(margem_adicional)      AS DOUBLE) / 100, 4)  AS margem_adicional_pct,
            trim(grupo_cics)                                                 AS grupo_cics
        FROM read_csv('s3://{{ var("bucket_lake") }}/margem_preferencia/CICS/res_7_CICS.csv', sep=';', all_varchar=true)
        WHERE trim(habilita) IS NOT NULL AND trim(habilita) != ''
            AND trim(margem_adicional) IS NOT NULL AND trim(margem_adicional) != ''

    UNION ALL

        -- Eventos de habilitação/desabilitação de margem normal (conteudo_nacional)
        SELECT
            'cics_8'                                                       AS resolucao,
            trim(habilita)                                                   AS tipo_evento_margem,
            'conteudo_nacional'::VARCHAR                                     AS tipo_margem,
            trim(ncm)                                                        AS prefixo_ncm,
            trim(regra_de_origem)                                            AS margem_normal_comprovante,
            round(TRY_CAST(trim(margem_normal)         AS DOUBLE) / 100, 4)  AS margem_normal_pct,
            NULL::VARCHAR                                                    AS margem_adicional_comprovante,
            NULL::DECIMAL(3,2)                                               AS margem_adicional_pct,
            trim(grupo_cics)                                                 AS grupo_cics
        FROM read_csv('s3://{{ var("bucket_lake") }}/margem_preferencia/CICS/res_8_CICS.csv', sep=';', all_varchar=true)
        WHERE trim(habilita) IS NOT NULL AND trim(habilita) != ''

        UNION ALL

        -- Eventos de habilitação/desabilitação de margem adicional (tecnologia_nacional)
        SELECT
            'cics_8'                                                       AS resolucao,
            trim(habilita)                                                   AS tipo_evento_margem,
            'tecnologia_nacional'::VARCHAR                                   AS tipo_margem,
            trim(ncm)                                                        AS prefixo_ncm,
            trim(regra_de_origem)                                            AS margem_normal_comprovante,
            round(TRY_CAST(trim(margem_normal)         AS DOUBLE) / 100, 4)  AS margem_normal_pct,
            replace(trim(regra_de_qualificacao), ' ', '_')                   AS margem_adicional_comprovante,
            round(TRY_CAST(trim(margem_adicional)      AS DOUBLE) / 100, 4)  AS margem_adicional_pct,
            trim(grupo_cics)                                                 AS grupo_cics
        FROM read_csv('s3://{{ var("bucket_lake") }}/margem_preferencia/CICS/res_8_CICS.csv', sep=';', all_varchar=true)
        WHERE trim(habilita) IS NOT NULL AND trim(habilita) != ''
            AND trim(margem_adicional) IS NOT NULL AND trim(margem_adicional) != ''

    UNION ALL

        -- Evento de habilitação de margem normal (conteudo_nacional) - só quando a margem já foi definida
        -- (a CIIA-PAC não tem desabilitação: toda linha só acrescenta NCM, por isso o evento é sempre 'habilita' fixo)
        SELECT
            'ciiapac_1'                                                                     AS resolucao,
            'habilita'::VARCHAR                                                            AS tipo_evento_margem,
            'conteudo_nacional'::VARCHAR                                                   AS tipo_margem,
            trim(ncm)                                                                      AS prefixo_ncm,
            regexp_replace(trim(regra_de_origem), '^c[oó]digo\s+', '', 'i')                AS margem_normal_comprovante,
            round(TRY_CAST(replace(trim(margem_normal), '%', '') AS DOUBLE) / 100, 4)      AS margem_normal_pct,
            NULL::VARCHAR                                                                  AS margem_adicional_comprovante,
            NULL::DECIMAL(3,2)                                                             AS margem_adicional_pct,
            trim(anexo)                                                                    AS grupo_cics
        FROM read_csv('s3://{{ var("bucket_lake") }}/margem_preferencia/CIIA-PAC/res_1_CIIA-PAC.csv', sep=';', all_varchar=true)
        WHERE trim(ncm) IS NOT NULL AND trim(ncm) != ''
            AND trim(margem_normal) IS NOT NULL AND trim(margem_normal) != ''

        UNION ALL

        -- Evento de habilitação de margem adicional (tecnologia_nacional) - só quando houver margem adicional
        SELECT
            'ciiapac_1'                                                                     AS resolucao,
            'habilita'::VARCHAR                                                            AS tipo_evento_margem,
            'tecnologia_nacional'::VARCHAR                                                 AS tipo_margem,
            trim(ncm)                                                                      AS prefixo_ncm,
            regexp_replace(trim(regra_de_origem), '^c[oó]digo\s+', '', 'i')                AS margem_normal_comprovante,
            round(TRY_CAST(replace(trim(margem_normal), '%', '') AS DOUBLE) / 100, 4)      AS margem_normal_pct,
            replace(trim(regra_de_qualificacao), ' ', '_')                                 AS margem_adicional_comprovante,
            round(TRY_CAST(replace(trim(margem_adicional), '%', '') AS DOUBLE) / 100, 4)   AS margem_adicional_pct,
            trim(anexo)                                                                    AS grupo_cics
        FROM read_csv('s3://{{ var("bucket_lake") }}/margem_preferencia/CIIA-PAC/res_1_CIIA-PAC.csv', sep=';', all_varchar=true)
        WHERE trim(ncm) IS NOT NULL AND trim(ncm) != ''
            AND trim(margem_adicional) IS NOT NULL AND trim(margem_adicional) != ''

    UNION ALL

        -- Evento de habilitação de margem normal (conteudo_nacional) - só quando a margem já foi definida
        SELECT
            'ciiapac_3'                                                                     AS resolucao,
            'habilita'::VARCHAR                                                            AS tipo_evento_margem,
            'conteudo_nacional'::VARCHAR                                                   AS tipo_margem,
            trim(ncm)                                                                      AS prefixo_ncm,
            regexp_replace(trim(regra_de_origem), '^c[oó]digo\s+', '', 'i')                AS margem_normal_comprovante,
            round(TRY_CAST(replace(trim(margem_normal), '%', '') AS DOUBLE) / 100, 4)      AS margem_normal_pct,
            NULL::VARCHAR                                                                  AS margem_adicional_comprovante,
            NULL::DECIMAL(3,2)                                                             AS margem_adicional_pct,
            trim(anexo)                                                                    AS grupo_cics
        FROM read_csv('s3://{{ var("bucket_lake") }}/margem_preferencia/CIIA-PAC/res_3_CIIA-PAC.csv', sep=';', all_varchar=true)
        WHERE trim(ncm) IS NOT NULL AND trim(ncm) != ''
            AND trim(margem_normal) IS NOT NULL AND trim(margem_normal) != ''

        UNION ALL

        -- Evento de habilitação de margem adicional (tecnologia_nacional) - só quando houver margem adicional
        SELECT
            'ciiapac_3'                                                                     AS resolucao,
            'habilita'::VARCHAR                                                            AS tipo_evento_margem,
            'tecnologia_nacional'::VARCHAR                                                 AS tipo_margem,
            trim(ncm)                                                                      AS prefixo_ncm,
            regexp_replace(trim(regra_de_origem), '^c[oó]digo\s+', '', 'i')                AS margem_normal_comprovante,
            round(TRY_CAST(replace(trim(margem_normal), '%', '') AS DOUBLE) / 100, 4)      AS margem_normal_pct,
            replace(trim(regra_de_qualificacao), ' ', '_')                                 AS margem_adicional_comprovante,
            round(TRY_CAST(replace(trim(margem_adicional), '%', '') AS DOUBLE) / 100, 4)   AS margem_adicional_pct,
            trim(anexo)                                                                    AS grupo_cics
        FROM read_csv('s3://{{ var("bucket_lake") }}/margem_preferencia/CIIA-PAC/res_3_CIIA-PAC.csv', sep=';', all_varchar=true)
        WHERE trim(ncm) IS NOT NULL AND trim(ncm) != ''
            AND trim(margem_adicional) IS NOT NULL AND trim(margem_adicional) != ''

    UNION ALL

        -- Evento de habilitação de margem normal (conteudo_nacional) - só quando a margem já foi definida
        SELECT
            'ciiapac_4'                                                                     AS resolucao,
            'habilita'::VARCHAR                                                            AS tipo_evento_margem,
            'conteudo_nacional'::VARCHAR                                                   AS tipo_margem,
            trim(ncm)                                                                      AS prefixo_ncm,
            regexp_replace(trim(regra_de_origem), '^c[oó]digo\s+', '', 'i')                AS margem_normal_comprovante,
            round(TRY_CAST(replace(trim(margem_normal), '%', '') AS DOUBLE) / 100, 4)      AS margem_normal_pct,
            NULL::VARCHAR                                                                  AS margem_adicional_comprovante,
            NULL::DECIMAL(3,2)                                                             AS margem_adicional_pct,
            trim(anexo)                                                                    AS grupo_cics
        FROM read_csv('s3://{{ var("bucket_lake") }}/margem_preferencia/CIIA-PAC/res_4_CIIA-PAC.csv', sep=';', all_varchar=true)
        WHERE trim(ncm) IS NOT NULL AND trim(ncm) != ''
            AND trim(margem_normal) IS NOT NULL AND trim(margem_normal) != ''

        UNION ALL

        -- Evento de habilitação de margem adicional (tecnologia_nacional) - só quando houver margem adicional
        SELECT
            'ciiapac_4'                                                                     AS resolucao,
            'habilita'::VARCHAR                                                            AS tipo_evento_margem,
            'tecnologia_nacional'::VARCHAR                                                 AS tipo_margem,
            trim(ncm)                                                                      AS prefixo_ncm,
            regexp_replace(trim(regra_de_origem), '^c[oó]digo\s+', '', 'i')                AS margem_normal_comprovante,
            round(TRY_CAST(replace(trim(margem_normal), '%', '') AS DOUBLE) / 100, 4)      AS margem_normal_pct,
            replace(trim(regra_de_qualificacao), ' ', '_')                                 AS margem_adicional_comprovante,
            round(TRY_CAST(replace(trim(margem_adicional), '%', '') AS DOUBLE) / 100, 4)   AS margem_adicional_pct,
            trim(anexo)                                                                    AS grupo_cics
        FROM read_csv('s3://{{ var("bucket_lake") }}/margem_preferencia/CIIA-PAC/res_4_CIIA-PAC.csv', sep=';', all_varchar=true)
        WHERE trim(ncm) IS NOT NULL AND trim(ncm) != ''
            AND trim(margem_adicional) IS NOT NULL AND trim(margem_adicional) != ''

    UNION ALL

        -- Evento de habilitação de margem normal (conteudo_nacional) - só quando a margem já foi definida
        SELECT
            'ciiapac_5'                                                                     AS resolucao,
            'habilita'::VARCHAR                                                            AS tipo_evento_margem,
            'conteudo_nacional'::VARCHAR                                                   AS tipo_margem,
            trim(ncm)                                                                      AS prefixo_ncm,
            regexp_replace(trim(regra_de_origem), '^c[oó]digo\s+', '', 'i')                AS margem_normal_comprovante,
            round(TRY_CAST(replace(trim(margem_normal), '%', '') AS DOUBLE) / 100, 4)      AS margem_normal_pct,
            NULL::VARCHAR                                                                  AS margem_adicional_comprovante,
            NULL::DECIMAL(3,2)                                                             AS margem_adicional_pct,
            trim(anexo)                                                                    AS grupo_cics
        FROM read_csv('s3://{{ var("bucket_lake") }}/margem_preferencia/CIIA-PAC/res_5_CIIA-PAC.csv', sep=';', all_varchar=true)
        WHERE trim(ncm) IS NOT NULL AND trim(ncm) != ''
            AND trim(margem_normal) IS NOT NULL AND trim(margem_normal) != ''

        UNION ALL

        -- Evento de habilitação de margem adicional (tecnologia_nacional) - só quando houver margem adicional
        SELECT
            'ciiapac_5'                                                                     AS resolucao,
            'habilita'::VARCHAR                                                            AS tipo_evento_margem,
            'tecnologia_nacional'::VARCHAR                                                 AS tipo_margem,
            trim(ncm)                                                                      AS prefixo_ncm,
            regexp_replace(trim(regra_de_origem), '^c[oó]digo\s+', '', 'i')                AS margem_normal_comprovante,
            round(TRY_CAST(replace(trim(margem_normal), '%', '') AS DOUBLE) / 100, 4)      AS margem_normal_pct,
            replace(trim(regra_de_qualificacao), ' ', '_')                                 AS margem_adicional_comprovante,
            round(TRY_CAST(replace(trim(margem_adicional), '%', '') AS DOUBLE) / 100, 4)   AS margem_adicional_pct,
            trim(anexo)                                                                    AS grupo_cics
        FROM read_csv('s3://{{ var("bucket_lake") }}/margem_preferencia/CIIA-PAC/res_5_CIIA-PAC.csv', sep=';', all_varchar=true)
        WHERE trim(ncm) IS NOT NULL AND trim(ncm) != ''
            AND trim(margem_adicional) IS NOT NULL AND trim(margem_adicional) != ''

),

-- Pega data_evento cruzando com tabela de resoluções, e já deixa coluna de margem_sustentabilidade_pct pronta pra ser preenchida depois
eventos AS (
    SELECT
        base.*,
        NULL::DECIMAL(3,2)               AS margem_sustentabilidade_pct,
        r.vigencia_inicio::DATE          AS data_evento
    FROM base
    JOIN {{ ref('stg_dim_margem_resolucoes') }} AS r ON r.id = base.resolucao
),

-- Particiona por prefixo_ncm + data_evento + tipo_evento_margem e ordena pela maior margem total
enumerated AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY prefixo_ncm, data_evento, tipo_evento_margem
        ORDER BY (margem_normal_pct + COALESCE(margem_adicional_pct, 0) + COALESCE(margem_sustentabilidade_pct, 0)) DESC
    ) AS rn
    FROM eventos
),

-- Pega só as linhas que o row number é 1 (maior margem total) pra evitar duplicidade
dedup AS (
    SELECT * EXCLUDE (rn)
    FROM enumerated
    WHERE rn = 1
)

-- Monta tabela fato final, sem duplicidades e pós cruzamento com resoluções pra pegar data_evento
SELECT
    ROW_NUMBER() OVER (ORDER BY data_evento, tipo_evento_margem, prefixo_ncm)
                                     AS id_evento_margem,
    tipo_evento_margem,
    tipo_margem,
    resolucao,
    data_evento,
    prefixo_ncm,
    margem_normal_comprovante,
    margem_normal_pct,
    margem_adicional_comprovante,
    margem_adicional_pct,
    margem_sustentabilidade_pct,
    grupo_cics
FROM dedup
ORDER BY data_evento, tipo_evento_margem, prefixo_ncm
