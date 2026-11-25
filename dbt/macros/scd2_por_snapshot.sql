{#
  Histórico de alterações (SCD2) a partir de snapshots completos acumulados num staging.

  Diferente dos modelos SCD2 do pncp_comprasgov (que usam a data de atualização de cada
  registro), aqui a fonte não diz quando cada registro mudou: cada carga é um retrato do
  cadastro inteiro. A versão de um registro muda quando:
    - o conteúdo muda entre dois snapshots consecutivos;
    - o registro some de um snapshot (a versão é fechada e, sem versão vigente, o registro
      deixa de aparecer nos marts); ou
    - o registro reaparece depois de ter sumido (abre nova versão).
  Snapshots consecutivos com conteúdo idêntico NÃO geram versão nova: uma versão cobre do
  primeiro snapshot em que aquele conteúdo apareceu até 1 microssegundo antes do snapshot
  em que ele mudou ou sumiu (fim inclusivo, como em int_ncm).

  Argumentos:
    relacao     ref() do staging, com uma linha por (chave, snapshot)
    chave       coluna que identifica o registro
    coluna_carga coluna TIMESTAMP com o momento do snapshot (vira valido_de)
    excluir     colunas técnicas do staging que não fazem parte do conteúdo
#}
{% macro scd2_por_snapshot(relacao, chave, coluna_carga='extraido_em', excluir=['_source_file', '_dbt_loaded_at']) %}

WITH staging AS (
    SELECT * EXCLUDE ({{ excluir | join(', ') }})
    FROM {{ relacao }}
),

{# O mesmo snapshot pode ter entrado mais de uma vez no staging (reprocessamento) #}
dedup AS (
    SELECT * FROM staging GROUP BY ALL
),

cargas AS (
    SELECT
        {{ coluna_carga }}                                   AS _carga,
        LEAD({{ coluna_carga }}) OVER (ORDER BY {{ coluna_carga }}) AS _proxima_carga
    FROM (SELECT DISTINCT {{ coluna_carga }} FROM dedup)
),

linhas AS (
    SELECT
        d.*,
        c._proxima_carga,
        md5(to_json(ROW(*COLUMNS(d.* EXCLUDE ({{ coluna_carga }}))))::VARCHAR) AS _hash
    FROM dedup d
    JOIN cargas c ON d.{{ coluna_carga }} = c._carga
),

marcadas AS (
    SELECT
        *,
        CASE
            WHEN LAG(_hash) OVER w IS NULL THEN TRUE                          -- primeira aparição
            WHEN LAG(_hash) OVER w <> _hash THEN TRUE                         -- conteúdo mudou
            WHEN LAG(_proxima_carga) OVER w <> {{ coluna_carga }} THEN TRUE   -- reapareceu após sumir
            ELSE FALSE
        END AS _nova_versao
    FROM linhas
    WINDOW w AS (PARTITION BY {{ chave }} ORDER BY {{ coluna_carga }})
),

versionadas AS (
    SELECT
        *,
        SUM(_nova_versao::INTEGER) OVER (
            PARTITION BY {{ chave }}
            ORDER BY {{ coluna_carga }}
            ROWS UNBOUNDED PRECEDING
        ) AS _versao
    FROM marcadas
),

{# Fim da versão: a carga seguinte à última em que a versão apareceu. Nula se a versão
   aparece no snapshot mais recente (vigente); preenchida se o conteúdo mudou ou se o
   registro sumiu do cadastro. #}
fins AS (
    SELECT
        {{ chave }},
        _versao,
        {# arg_max_null, e não arg_max: arg_max ignora linhas com valor nulo, e o nulo
           (versão presente no último snapshot) é justamente o que indica a vigência #}
        arg_max_null(_proxima_carga, {{ coluna_carga }}) AS _fim,
        max({{ coluna_carga }})                     AS ultimo_snapshot
    FROM versionadas
    GROUP BY ALL
)

SELECT
    v.* EXCLUDE (_proxima_carga, _hash, _nova_versao, _versao, {{ coluna_carga }}),
    v.{{ coluna_carga }}                     AS valido_de,
    f._fim - INTERVAL '1 microsecond'        AS valido_ate,
    f._fim IS NULL                           AS is_current,
    f.ultimo_snapshot
FROM versionadas v
JOIN fins f USING ({{ chave }}, _versao)
WHERE v._nova_versao

{% endmacro %}
