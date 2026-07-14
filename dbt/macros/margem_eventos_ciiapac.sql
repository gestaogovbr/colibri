{% macro margem_eventos_ciiapac(resolucao, aba, tem_anexo=true) %}
    {% set path = 's3://' ~ var('bucket_arquivos') ~ '/raw/resolucoes_cics_e_ciia_pac/' ~ var('margem_arquivo_resolucoes') %}

    SELECT
        '{{ resolucao }}'                                                               AS resolucao,
        'habilita'::VARCHAR                                                            AS tipo_evento_margem,
        'conteudo_nacional'::VARCHAR                                                   AS tipo_margem,
        trim(ncm)                                                                      AS prefixo_ncm,
        regexp_replace(trim(regra_de_origem), '^c[oó]digo\s+', '', 'i')                AS margem_normal_comprovante,
        round(TRY_CAST(trim(margem_normal) AS DOUBLE), 4)                              AS margem_normal_pct,
        NULL::VARCHAR                                                                  AS margem_adicional_comprovante,
        NULL::DECIMAL(3,2)                                                             AS margem_adicional_pct,
        {{ "trim(anexo)" if tem_anexo else "NULL::VARCHAR" }}                          AS grupo_cics
    FROM read_xlsx('{{ path }}', sheet='{{ aba }}', all_varchar=true)
    WHERE trim(ncm) IS NOT NULL AND trim(ncm) != ''
        AND trim(margem_normal) IS NOT NULL AND trim(margem_normal) != ''

    UNION ALL

    SELECT
        '{{ resolucao }}'                                                               AS resolucao,
        'habilita'::VARCHAR                                                            AS tipo_evento_margem,
        'tecnologia_nacional'::VARCHAR                                                 AS tipo_margem,
        trim(ncm)                                                                      AS prefixo_ncm,
        regexp_replace(trim(regra_de_origem), '^c[oó]digo\s+', '', 'i')                AS margem_normal_comprovante,
        round(TRY_CAST(trim(margem_normal) AS DOUBLE), 4)                              AS margem_normal_pct,
        replace(trim(regra_de_qualificacao), ' ', '_')                                 AS margem_adicional_comprovante,
        round(TRY_CAST(trim(margem_adicional) AS DOUBLE), 4)                           AS margem_adicional_pct,
        {{ "trim(anexo)" if tem_anexo else "NULL::VARCHAR" }}                          AS grupo_cics
    FROM read_xlsx('{{ path }}', sheet='{{ aba }}', all_varchar=true)
    WHERE trim(ncm) IS NOT NULL AND trim(ncm) != ''
        AND trim(margem_adicional) IS NOT NULL AND trim(margem_adicional) != ''
{% endmacro %}
