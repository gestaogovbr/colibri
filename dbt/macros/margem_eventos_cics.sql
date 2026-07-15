{% macro margem_eventos_cics(resolucao, anterior=none) %}
    {% set aba = 'res_cics_' ~ resolucao.split('_')[-1] %}
    {% set path = 's3://' ~ var('bucket_arquivos') ~ '/raw/resolucoes_cics_e_ciia_pac/' ~ var('margem_arquivo_resolucoes') %}

    SELECT
        '{{ resolucao }}'                                                AS resolucao,
        'habilita'::VARCHAR                                              AS tipo_evento_margem,
        'conteudo_nacional'::VARCHAR                                     AS tipo_margem,
        trim(ncm)                                                        AS prefixo_ncm,
        trim(regra_de_origem)                                            AS margem_normal_comprovante,
        round(TRY_CAST(trim(margem_normal)         AS DOUBLE) / 100, 4)  AS margem_normal_pct,
        NULL::VARCHAR                                                    AS margem_adicional_comprovante,
        NULL::DECIMAL(3,2)                                               AS margem_adicional_pct,
        trim(grupo_de_produtos)                                                 AS grupo_de_produtos
    FROM read_xlsx('{{ path }}', sheet='{{ aba }}', all_varchar=true)
    WHERE trim(resolucao) = '{{ resolucao }}' AND trim(ncm) IS NOT NULL AND trim(ncm) != ''

    UNION ALL

    SELECT
        '{{ resolucao }}'                                                AS resolucao,
        'habilita'::VARCHAR                                              AS tipo_evento_margem,
        'tecnologia_nacional'::VARCHAR                                   AS tipo_margem,
        trim(ncm)                                                        AS prefixo_ncm,
        trim(regra_de_origem)                                            AS margem_normal_comprovante,
        round(TRY_CAST(trim(margem_normal)         AS DOUBLE) / 100, 4)  AS margem_normal_pct,
        replace(trim(regra_de_qualificacao), ' ', '_')                   AS margem_adicional_comprovante,
        round(TRY_CAST(trim(margem_adicional)      AS DOUBLE) / 100, 4)  AS margem_adicional_pct,
        trim(grupo_de_produtos)                                                 AS grupo_de_produtos
    FROM read_xlsx('{{ path }}', sheet='{{ aba }}', all_varchar=true)
    WHERE trim(resolucao) = '{{ resolucao }}' AND trim(ncm) IS NOT NULL AND trim(ncm) != ''
        AND trim(margem_adicional) IS NOT NULL AND trim(margem_adicional) != ''

    {% if anterior %}
    {% set aba_anterior = 'res_cics_' ~ anterior.split('_')[-1] %}

    UNION ALL

    SELECT
        '{{ resolucao }}'                                                AS resolucao,
        'desabilita'::VARCHAR                                            AS tipo_evento_margem,
        'conteudo_nacional'::VARCHAR                                     AS tipo_margem,
        trim(ncm)                                                        AS prefixo_ncm,
        trim(regra_de_origem)                                            AS margem_normal_comprovante,
        round(TRY_CAST(trim(margem_normal)         AS DOUBLE) / 100, 4)  AS margem_normal_pct,
        NULL::VARCHAR                                                    AS margem_adicional_comprovante,
        NULL::DECIMAL(3,2)                                               AS margem_adicional_pct,
        trim(grupo_de_produtos)                                          AS grupo_de_produtos
    FROM read_xlsx('{{ path }}', sheet='{{ aba_anterior }}', all_varchar=true)
    WHERE trim(resolucao) = '{{ anterior }}' AND trim(ncm) IS NOT NULL AND trim(ncm) != ''

    UNION ALL

    SELECT
        '{{ resolucao }}'                                                AS resolucao,
        'desabilita'::VARCHAR                                            AS tipo_evento_margem,
        'tecnologia_nacional'::VARCHAR                                   AS tipo_margem,
        trim(ncm)                                                        AS prefixo_ncm,
        trim(regra_de_origem)                                            AS margem_normal_comprovante,
        round(TRY_CAST(trim(margem_normal)         AS DOUBLE) / 100, 4)  AS margem_normal_pct,
        replace(trim(regra_de_qualificacao), ' ', '_')                   AS margem_adicional_comprovante,
        round(TRY_CAST(trim(margem_adicional)      AS DOUBLE) / 100, 4)  AS margem_adicional_pct,
        trim(grupo_de_produtos)                                          AS grupo_de_produtos
    FROM read_xlsx('{{ path }}', sheet='{{ aba_anterior }}', all_varchar=true)
    WHERE trim(resolucao) = '{{ anterior }}' AND trim(ncm) IS NOT NULL AND trim(ncm) != ''
        AND trim(margem_adicional) IS NOT NULL AND trim(margem_adicional) != ''
    {% endif %}
{% endmacro %}
