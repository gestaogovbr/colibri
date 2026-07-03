import duckdb
import os
from pathlib import Path

from utils.constantes import RAIZ_PROJETO

DADOS = Path(RAIZ_PROJETO) / "dados" / "margem_preferencia"
DB_PATH = DADOS / "margem.duckdb"
RESULTS_PATH = DADOS

con = duckdb.connect(DB_PATH)
con.execute("""
    CREATE OR REPLACE TABLE dim_margem_ncm_prefixos AS

    -- Puxa eventos da fato_margem_eventos cruzando com resoluções pra ter data_evento, e renomeia colunas de margem
    WITH eventos AS (
        SELECT
            e.prefixo_ncm,
            e.resolucao,
            e.tipo_margem,
            CAST(r.vigencia_inicio AS DATE) AS data_evento,
            e.tipo_evento_margem,
            e.margem_normal_comprovante,
            e.margem_normal_pct,
            e.margem_adicional_comprovante,
            e.margem_adicional_pct,
            NULL::VARCHAR                              AS margem_sustentabilidade_comprovante,
            NULL::DECIMAL(3,2)                         AS margem_sustentabilidade_pct
        FROM fato_margem_eventos e
        JOIN dim_margem_resolucoes r
            ON e.resolucao = r.id
    ),

    -- Calcula a data do próximo evento por NCM + resolução + tipo_margem pra definir o fim do intervalo
    eventos_ordenados AS (
        SELECT
            *,
            LEAD(data_evento) OVER (
                PARTITION BY prefixo_ncm, tipo_margem
                -- desabilita antes de habilita no mesmo dia: evita que o LEAD do habilita aponte para o desabilita do mesmo dia
                ORDER BY data_evento, tipo_evento_margem ASC
            ) AS proxima_data_evento
        FROM eventos
    ),

    -- Filtra só os eventos de habilita e transforma em intervalos (data_inicio -> data_fim)
    intervalos AS (
        SELECT
            prefixo_ncm,
            resolucao,
            tipo_margem,
            data_evento                    AS data_inicio,
            proxima_data_evento - INTERVAL 1 DAY AS data_fim,
            margem_normal_comprovante,
            margem_normal_pct,
            margem_adicional_comprovante,
            margem_adicional_pct,
            margem_sustentabilidade_comprovante,
            margem_sustentabilidade_pct
        FROM eventos_ordenados
        WHERE tipo_evento_margem = 'habilita'
    ),

    -- Numera os intervalos por NCM do mais recente pro mais antigo, pra identificar o último
    numerados AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY prefixo_ncm
                ORDER BY data_inicio DESC
            ) AS rn
        FROM intervalos
    ),

    -- Marca ultima (registro mais recente por NCM) e ativa (ultima sem data_fim, ou seja, não desabilitada)
    marcadores AS (
        SELECT * EXCLUDE (rn),
            rn = 1                    AS ultima,
            rn = 1 AND data_fim IS NULL AS ativa
        FROM numerados
    ),

    -- Particiona por prefixo_ncm + data_inicio e ordena pela maior margem total pra deduplicar
    enumerated AS (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY prefixo_ncm, data_inicio
            ORDER BY (margem_normal_pct + margem_adicional_pct + margem_sustentabilidade_pct) DESC
        ) AS rn
        FROM marcadores
    ),

    -- Pega só as linhas que o row number é 1 (maior margem total) pra evitar duplicidade
    dedup AS (
        SELECT * EXCLUDE(rn)
        FROM enumerated
        WHERE rn = 1
    )

    -- Monta tabela dimensão final, sem duplicidades, com marcadores de ultima e ativa
    SELECT * FROM dedup
""")

n = con.execute("SELECT COUNT(*) FROM dim_margem_ncm_prefixos").fetchone()[0]
ativos = con.execute(
    "SELECT COUNT(*) FROM dim_margem_ncm_prefixos WHERE ativa"
).fetchone()[0]

df = con.execute("SELECT * FROM dim_margem_ncm_prefixos").df()
df.to_csv(os.path.join(RESULTS_PATH, "dim_margem_ncm_prefixos_utf8.csv"), index=False, lineterminator="\n")

con.close()

print(f"OK: {n} registros | {ativos} ativos")
