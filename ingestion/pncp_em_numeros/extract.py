"""
Extração das tabelas/views "PNCP em números" via Databricks SQL.

Lê 4 tabelas brutas (catalog raw, schema geral) e 3 views agregadas (catalog
cotin_dlt_pncp, schema bronze) e salva cada uma como parquet em
dados/pncp_em_numeros/.

Manifesto incremental (pncp_em_numeros_manifesto.csv):
  Toda execução consulta a tabela/view inteira e recalcula o hash SHA-256 do
  conteúdo (não há filtro incremental do lado do Databricks). Fontes com hash
  inalterado não são regravadas. Várias dessas fontes têm dezenas de milhões de
  linhas (ex: faseexterna_proposta_item ~46M, vw_agg_itens_bronze ~39M) — por
  isso o resultado é lido via fetchall_arrow() e o hash é calculado sobre os
  bytes do parquet já serializado, evitando iterar linha a linha em Python.
  O manifesto é sincronizado com o bucket para persistir entre ambientes.

Uso:
  python -m ingestion.pncp_em_numeros.extract
"""

import csv
import hashlib
import io
import logging
import shutil
from datetime import datetime
from pathlib import Path

import pyarrow.parquet as pq
from databricks import sql

import shared.configurar_logging as log
from shared.baixar_arquivo import baixar_arquivo_do_bucket
from shared.carregar_segredo import carregar_segredo
from shared.salvar_arquivo import salvar_arquivo_no_bucket

log.setup_logging()
logger = logging.getLogger(__name__)

SEGREDO_DATABRICKS = "databricks-cotin"
SEGREDO_BUCKET = "colibri-token-desenvolvedor"

# nome_logico -> (catalog, schema, tabela_ou_view)
FONTES = {
    #"compra":        ("raw", "geral", "faseexterna_compra"),
    #"item":          ("raw", "geral", "faseexterna_item"),
    #"participacao":  ("raw", "geral", "faseexterna_participacao"),
    #"proposta_item": ("raw", "geral", "faseexterna_proposta_item"),
    "agg_compra":    ("cotin_dlt_pncp", "bronze", "vw_agg_compra_bronze"),
    "agg_itens":     ("cotin_dlt_pncp", "bronze", "vw_agg_itens_bronze"),
    "agg_resultado": ("cotin_dlt_pncp", "bronze", "vw_agg_resultado_bronze"),
}

DIRETORIO_DADOS = Path("./dados")
DIRETORIO_SAIDA = DIRETORIO_DADOS / "pncp_em_numeros"
DIRETORIO_MANIFESTOS = DIRETORIO_DADOS / "manifestos"
DIRETORIO_ALTERACOES = DIRETORIO_DADOS / "alteracoes"

NOME_MANIFESTO = "pncp_em_numeros_manifesto.csv"
COLUNAS_MANIFESTO = ["tabela", "hash_sha256", "num_linhas", "extraido_em"]

NOME_ALTERACOES = "pncp_em_numeros_alteracoes.csv"
COLUNAS_ALTERACOES = ["tabela"]


def conectar_databricks():
    config = carregar_segredo(SEGREDO_DATABRICKS)
    return sql.connect(
        server_hostname=config["server_hostname"],
        http_path=config["http_path"],
        access_token=config["access_token"],
    )


def carregar_manifesto(caminho: Path) -> dict[str, dict]:
    if not caminho.exists():
        return {}
    with open(caminho, newline="", encoding="utf-8") as f:
        return {r["tabela"]: r for r in csv.DictReader(f)}


def salvar_manifesto(caminho: Path, manifesto: dict[str, dict]) -> None:
    with open(caminho, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUNAS_MANIFESTO)
        w.writeheader()
        w.writerows(sorted(manifesto.values(), key=lambda e: e["tabela"]))


def salvar_alteracoes(caminho: Path, alteracoes: list[str]) -> None:
    with open(caminho, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(COLUNAS_ALTERACOES)
        for tabela in alteracoes:
            w.writerow([tabela])


def _tabela_para_parquet_bytes_e_hash(tabela_arrow) -> tuple[bytes, str]:
    buffer = io.BytesIO()
    pq.write_table(tabela_arrow, buffer)
    bytes_parquet = buffer.getvalue()
    return bytes_parquet, hashlib.sha256(bytes_parquet).hexdigest()


def resetar_dados_locais() -> None:
    """Apaga os parquets extraídos e o manifesto local (usado por --do-zero)"""
    shutil.rmtree(DIRETORIO_SAIDA, ignore_errors=True)
    (DIRETORIO_MANIFESTOS / NOME_MANIFESTO).unlink(missing_ok=True)
    (DIRETORIO_ALTERACOES / NOME_ALTERACOES).unlink(missing_ok=True)


def subir_manifesto() -> None:
    """Sobe o manifesto local pro bucket. Só deve ser chamado depois do dbt rodar com sucesso"""
    caminho_manifesto = DIRETORIO_MANIFESTOS / NOME_MANIFESTO
    if not caminho_manifesto.exists():
        return
    bucket = carregar_segredo(SEGREDO_BUCKET)["bucket_lake"]
    try:
        salvar_arquivo_no_bucket(str(caminho_manifesto), bucket, SEGREDO_BUCKET, NOME_MANIFESTO)
    except Exception as e:
        logger.warning(f"Não foi possível salvar manifesto no bucket: {e}")


def executar_ingestao() -> bool:
    """Retorna True se algum dado novo foi extraído, False se nada mudou"""
    DIRETORIO_SAIDA.mkdir(parents=True, exist_ok=True)
    DIRETORIO_MANIFESTOS.mkdir(parents=True, exist_ok=True)
    DIRETORIO_ALTERACOES.mkdir(parents=True, exist_ok=True)

    caminho_manifesto = DIRETORIO_MANIFESTOS / NOME_MANIFESTO
    caminho_alteracoes = DIRETORIO_ALTERACOES / NOME_ALTERACOES
    bucket = carregar_segredo(SEGREDO_BUCKET)["bucket_lake"]

    caminho_manifesto.unlink(missing_ok=True)
    try:
        baixar_arquivo_do_bucket(NOME_MANIFESTO, bucket, SEGREDO_BUCKET, str(caminho_manifesto))
        logger.info(f"Manifesto baixado do bucket: {bucket}/{NOME_MANIFESTO}")
    except Exception as e:
        logger.warning(f"Manifesto não encontrado no bucket, iniciando do zero: {e}")

    manifesto = carregar_manifesto(caminho_manifesto)
    alteracoes: list[str] = []
    manifesto_modificado = False

    conn = conectar_databricks()
    try:
        cursor = conn.cursor()
        try:
            for nome_logico, (catalogo, schema, nome_objeto) in FONTES.items():
                logger.info(f"Consultando {catalogo}.{schema}.{nome_objeto}...")
                cursor.execute(f"SELECT * FROM {catalogo}.{schema}.{nome_objeto}")
                tabela_arrow = cursor.fetchall_arrow()
                bytes_parquet, hash_atual = _tabela_para_parquet_bytes_e_hash(tabela_arrow)

                caminho_parquet = DIRETORIO_SAIDA / f"{nome_logico}.parquet"
                entrada = manifesto.get(nome_logico)
                if entrada and entrada["hash_sha256"] == hash_atual and caminho_parquet.exists():
                    logger.info(f"{nome_logico}: hash bate com manifesto, pulando")
                    continue

                caminho_parquet.write_bytes(bytes_parquet)

                manifesto[nome_logico] = {
                    "tabela": nome_logico,
                    "hash_sha256": hash_atual,
                    "num_linhas": tabela_arrow.num_rows,
                    "extraido_em": datetime.now().isoformat(timespec="seconds"),
                }
                alteracoes.append(nome_logico)
                manifesto_modificado = True
                logger.info(f"{nome_logico}: {tabela_arrow.num_rows:,} linha(s) gravada(s) em {caminho_parquet}")
        finally:
            cursor.close()
    finally:
        conn.close()

    salvar_manifesto(caminho_manifesto, manifesto)
    salvar_alteracoes(caminho_alteracoes, alteracoes)
    logger.info(f"Manifesto: {caminho_manifesto} ({len(manifesto)} fonte(s))")
    logger.info(f"Alterações: {caminho_alteracoes} ({len(alteracoes)} fonte(s))")

    return manifesto_modificado


if __name__ == "__main__":
    executar_ingestao()
