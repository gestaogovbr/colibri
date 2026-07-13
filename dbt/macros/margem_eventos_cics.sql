{#
    Gera os 2 SELECTs de eventos (margem normal/conteudo_nacional e margem
    adicional/tecnologia_nacional) de uma resolução CICS a partir do seu id.

    Assume que toda resolução `cics_N` tem o arquivo correspondente em
    `margem_preferencia/CICS/res_N_CICS.csv` no bucket.
#}
{% macro margem_eventos_cics(resolucao) %}
    {% set numero = resolucao.split('_')[-1] %}
    {% set path = 's3://' ~ var('bucket_lake') ~ '/margem_preferencia/CICS/res_' ~ numero ~ '_CICS.csv' %}

    -- Eventos de habilitação/desabilitação de margem normal (conteudo_nacional)
    SELECT
        '{{ resolucao }}'                                                AS resolucao,
        trim(habilita)                                                   AS tipo_evento_margem,
        'conteudo_nacional'::VARCHAR                                     AS tipo_margem,
        trim(ncm)                                                        AS prefixo_ncm,
        trim(regra_de_origem)                                            AS margem_normal_comprovante,
        round(TRY_CAST(trim(margem_normal)         AS DOUBLE) / 100, 4)  AS margem_normal_pct,
        NULL::VARCHAR                                                    AS margem_adicional_comprovante,
        NULL::DECIMAL(3,2)                                               AS margem_adicional_pct,
        trim(grupo_cics)                                                 AS grupo_cics
    FROM read_csv('{{ path }}', sep=';', all_varchar=true)
    WHERE trim(habilita) IS NOT NULL AND trim(habilita) != ''

    UNION ALL

    -- Eventos de habilitação/desabilitação de margem adicional (tecnologia_nacional)
    SELECT
        '{{ resolucao }}'                                                AS resolucao,
        trim(habilita)                                                   AS tipo_evento_margem,
        'tecnologia_nacional'::VARCHAR                                   AS tipo_margem,
        trim(ncm)                                                        AS prefixo_ncm,
        trim(regra_de_origem)                                            AS margem_normal_comprovante,
        round(TRY_CAST(trim(margem_normal)         AS DOUBLE) / 100, 4)  AS margem_normal_pct,
        replace(trim(regra_de_qualificacao), ' ', '_')                   AS margem_adicional_comprovante,
        round(TRY_CAST(trim(margem_adicional)      AS DOUBLE) / 100, 4)  AS margem_adicional_pct,
        trim(grupo_cics)                                                 AS grupo_cics
    FROM read_csv('{{ path }}', sep=';', all_varchar=true)
    WHERE trim(habilita) IS NOT NULL AND trim(habilita) != ''
        AND trim(margem_adicional) IS NOT NULL AND trim(margem_adicional) != ''
{% endmacro %}
