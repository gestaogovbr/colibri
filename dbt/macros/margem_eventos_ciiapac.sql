{#
    Gera os 2 SELECTs de eventos (margem normal/conteudo_nacional e margem
    adicional/tecnologia_nacional) de uma resolução CIIA-PAC a partir do seu id.

    A CIIA-PAC não tem desabilitação; assume que toda resolução `ciiapac_N` tem o 
    arquivo correspondente em `margem_preferencia/CIIA-PAC/res_N_CIIA-PAC.csv` no bucket.
#}
{% macro margem_eventos_ciiapac(resolucao) %}
    {% set numero = resolucao.split('_')[-1] %}
    {% set path = 's3://' ~ var('bucket_lake') ~ '/margem_preferencia/CIIA-PAC/res_' ~ numero ~ '_CIIA-PAC.csv' %}

    -- Evento de habilitação de margem normal (conteudo_nacional) - só quando a margem já foi definida
    SELECT
        '{{ resolucao }}'                                                               AS resolucao,
        'habilita'::VARCHAR                                                            AS tipo_evento_margem,
        'conteudo_nacional'::VARCHAR                                                   AS tipo_margem,
        trim(ncm)                                                                      AS prefixo_ncm,
        regexp_replace(trim(regra_de_origem), '^c[oó]digo\s+', '', 'i')                AS margem_normal_comprovante,
        round(TRY_CAST(replace(trim(margem_normal), '%', '') AS DOUBLE) / 100, 4)      AS margem_normal_pct,
        NULL::VARCHAR                                                                  AS margem_adicional_comprovante,
        NULL::DECIMAL(3,2)                                                             AS margem_adicional_pct,
        trim(anexo)                                                                    AS grupo_cics
    FROM read_csv('{{ path }}', sep=';', all_varchar=true)
    WHERE trim(ncm) IS NOT NULL AND trim(ncm) != ''
        AND trim(margem_normal) IS NOT NULL AND trim(margem_normal) != ''

    UNION ALL

    -- Evento de habilitação de margem adicional (tecnologia_nacional) - só quando houver margem adicional
    SELECT
        '{{ resolucao }}'                                                               AS resolucao,
        'habilita'::VARCHAR                                                            AS tipo_evento_margem,
        'tecnologia_nacional'::VARCHAR                                                 AS tipo_margem,
        trim(ncm)                                                                      AS prefixo_ncm,
        regexp_replace(trim(regra_de_origem), '^c[oó]digo\s+', '', 'i')                AS margem_normal_comprovante,
        round(TRY_CAST(replace(trim(margem_normal), '%', '') AS DOUBLE) / 100, 4)      AS margem_normal_pct,
        replace(trim(regra_de_qualificacao), ' ', '_')                                 AS margem_adicional_comprovante,
        round(TRY_CAST(replace(trim(margem_adicional), '%', '') AS DOUBLE) / 100, 4)   AS margem_adicional_pct,
        trim(anexo)                                                                    AS grupo_cics
    FROM read_csv('{{ path }}', sep=';', all_varchar=true)
    WHERE trim(ncm) IS NOT NULL AND trim(ncm) != ''
        AND trim(margem_adicional) IS NOT NULL AND trim(margem_adicional) != ''
{% endmacro %}
