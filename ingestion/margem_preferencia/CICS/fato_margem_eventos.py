import duckdb
import os
from pathlib import Path

from utils.constantes import RAIZ_PROJETO

DADOS = Path(RAIZ_PROJETO) / "dados" / "margem_preferencia"
DOWNLOADS_PATH = DADOS / "CICS"
DB_PATH = DADOS / "margem.duckdb"
RESULTS_PATH = DADOS

ARQUIVOS = [
    ("res_1_CICS", "cics_1"),
    ("res_3_CICS", "cics_3"),
    ("res_4_CICS", "cics_4"),
    ("res_7_CICS", "cics_7"),
    ("res_8_CICS", "cics_8"),
]


def csv_query(nome, res_id):
    # forward slashes obrigatórios dentro de string SQL no Windows
    path = str(DOWNLOADS_PATH / f"{nome}.csv").replace("\\", "/")

    # Lê o CSV e filtra linhas sem habilita
    from_where = f"""
        FROM read_csv('{path}', sep=';', all_varchar=true)
        WHERE trim(habilita) IS NOT NULL AND trim(habilita) != ''"""

    # Retorna string SQL com 2 selects (conteudo_nacional, tecnologia_nacional) e com tratamento de margens
    return f"""
        -- Eventos de habilitação/desabilitação de margem normal (conteudo_nacional)
        SELECT
            '{res_id}'                                                       AS resolucao,
            trim(habilita)                                                   AS tipo_evento_margem,
            'conteudo_nacional'::VARCHAR                                     AS tipo_margem,
            trim(ncm)                                                        AS prefixo_ncm,
            trim(regra_de_origem)                                            AS margem_normal_comprovante,
            round(TRY_CAST(trim(margem_normal)         AS DOUBLE) / 100, 4)  AS margem_normal_pct,
            NULL::VARCHAR                                                    AS margem_adicional_comprovante,
            NULL::DECIMAL(3,2)                                               AS margem_adicional_pct,
            trim(grupo_cics)                                                 AS grupo_cics
        {from_where}

        UNION ALL

        -- Eventos de habilitação/desabilitação de margem adicional (tecnologia_nacional)
        SELECT
            '{res_id}'                                                       AS resolucao,
            trim(habilita)                                                   AS tipo_evento_margem,
            'tecnologia_nacional'::VARCHAR                                   AS tipo_margem,
            trim(ncm)                                                        AS prefixo_ncm,
            trim(regra_de_origem)                                            AS margem_normal_comprovante,
            round(TRY_CAST(trim(margem_normal)         AS DOUBLE) / 100, 4)  AS margem_normal_pct,
            replace(trim(regra_de_qualificacao), ' ', '_')                   AS margem_adicional_comprovante,
            round(TRY_CAST(trim(margem_adicional)      AS DOUBLE) / 100, 4)  AS margem_adicional_pct,
            trim(grupo_cics)                                                 AS grupo_cics
        {from_where}
            AND trim(margem_adicional) IS NOT NULL AND trim(margem_adicional) != ''
    """


con = duckdb.connect(DB_PATH)

con.execute("DROP TABLE IF EXISTS dim_margem_ncm_prefixos")
con.execute("DROP TABLE IF EXISTS fato_margem_eventos")

# Cria tabela fato_margem_eventos
con.execute("""
    CREATE TABLE fato_margem_eventos (
        id_evento_margem             INTEGER PRIMARY KEY,
        tipo_evento_margem           VARCHAR CHECK (tipo_evento_margem IN ('habilita', 'desabilita')),
        tipo_margem                  VARCHAR CHECK (tipo_margem IN (
                                         'conteudo_nacional',
                                         'tecnologia_nacional',
                                         'sustentabilidade'
                                     )),
        resolucao                    VARCHAR REFERENCES dim_margem_resolucoes(id),
        data_evento                  DATE NOT NULL,
        prefixo_ncm                  VARCHAR CHECK (length(prefixo_ncm) IN (2, 4, 5, 6, 7, 8)),
        margem_normal_comprovante    VARCHAR, CHECK (margem_normal_comprovante IN ('CFI', 'CFI-A', 'MedNac', 'CFI ou PPB')),
        margem_normal_pct            DECIMAL(3,2) CHECK (margem_normal_pct BETWEEN 0.00 AND 1.00),
        margem_adicional_comprovante VARCHAR CHECK (margem_adicional_comprovante IS NULL OR
                                         margem_adicional_comprovante IN ('MedIFANac', 'portaria_DesIn')),
        margem_adicional_pct         DECIMAL(3,2) CHECK (margem_adicional_pct BETWEEN 0.00 AND 1.00),
        margem_sustentabilidade_pct  DECIMAL(3,2) CHECK (margem_sustentabilidade_pct BETWEEN 0.00 AND 1.00),
        grupo_cics                   VARCHAR
    )
""")

# Junta cada tabela de evento (res1, res3, res4, res7, res8) numa única string SQL
union_sql = "\nUNION ALL\n".join(
    csv_query(res_arquivo, res_id) for res_arquivo, res_id in ARQUIVOS
)

# Insere dados na fato_margem_eventos, dando preferência pra maior margem_total quando houver duplicidade
con.execute(f"""
    INSERT INTO fato_margem_eventos

    -- Monta tabela base UNINDO os dados de eventos de cada resolução (res1, res3...)
    WITH base AS (
        {union_sql}
    ),

    -- Pega data_evento cruzando com tabela de resoluções, e já deixa coluna de margem_sustentabilidade_pct pronta pra ser preenchida depois
    eventos AS (
        SELECT
            base.*,
            NULL::DECIMAL(3,2)               AS margem_sustentabilidade_pct,
            r.vigencia_inicio::DATE          AS data_evento
        FROM base
        JOIN dim_margem_resolucoes AS r ON r.id = base.resolucao
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
""")

n = con.execute("SELECT COUNT(*) FROM fato_margem_eventos").fetchone()[0]

df = con.execute("SELECT * FROM fato_margem_eventos").df()
df.to_csv(os.path.join(RESULTS_PATH, "fato_margem_eventos_utf8.csv"), index=False, lineterminator="\n")

rows = con.execute("""
    SELECT resolucao, tipo_evento_margem, COUNT(*) AS qtd
    FROM fato_margem_eventos
    GROUP BY resolucao, tipo_evento_margem
    ORDER BY resolucao, tipo_evento_margem
""").fetchall()
con.close()

print(f"OK: {n} eventos inseridos")
for resolucao, tipo, qtd in rows:
    print(f"  {resolucao} / {tipo}: {qtd}")
