"""
Extração incremental de compras do Databricks (cotin_dlt_pncp.bronze.vw_agg_compra_bronze)
para o lake S3/R2. Cada execução salva as linhas novas ou atualizadas como um arquivo
Parquet com timestamp, acumulando o histórico completo de versões.

Controle de posição: o max(id_dt_atualizacao) da última execução bem-sucedida é gravado
em .colibri_state/pncp_last_run.json. Se o arquivo não existir, faz carga inicial completa.
"""
import io
import json
import logging
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from databricks import sql as databricks_sql

from shared.carregar_segredo import carregar_segredo
import shared.configurar_logging as log


log.setup_logging()
logger = logging.getLogger(__name__)

BUCKET = "colibri-dev"
PREFIXO_S3 = "raw/pncp/compras"
TABELA_FONTE = "cotin_dlt_pncp.bronze.vw_agg_compra_bronze"
SEGREDO_S3 = "colibri-token-desenvolvedor"
SEGREDO_DATABRICKS = "databricks-pncp"
FUSO = ZoneInfo("America/Sao_Paulo")
STATE_FILE = Path(".colibri_state/pncp_last_run.json")


def _cliente_s3(config: dict):
    return boto3.client(
        "s3",
        endpoint_url=config["endpoint"],
        aws_access_key_id=config["access_key"],
        aws_secret_access_key=config["secret_key"],
        region_name="auto",
    )


def ler_estado() -> dict | None:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return None


def salvar_estado(ultimo_ts, uri: str) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps({
        "ultimo_ts": str(ultimo_ts),
        "uri": uri,
        "run_at": datetime.now(FUSO).isoformat(),
    }, indent=2))


def extrair_do_databricks(config_db: dict, ultimo_ts) -> pa.Table:
    """Consulta o Databricks e retorna linhas novas/atualizadas como PyArrow Table."""
    where = ""
    if ultimo_ts is not None:
        where = f"WHERE id_dt_atualizacao > CAST('{ultimo_ts}' AS TIMESTAMP)"

    query = f"SELECT * FROM {TABELA_FONTE} {where}".strip()
    logger.info(f"Query: {query[:300]}")

    with databricks_sql.connect(
        server_hostname=config_db["server_hostname"],
        http_path=config_db["http_path"],
        access_token=config_db["access_token"],
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            table = cursor.fetchall_arrow()

    logger.info(f"Extraídas {table.num_rows:,} linhas do Databricks")
    return table


def salvar_no_s3(table: pa.Table, config_s3: dict, ts_execucao: str) -> str:
    """Serializa como Parquet (snappy) e faz upload para S3."""
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    buffer.seek(0)

    chave = f"{PREFIXO_S3}/{ts_execucao}.parquet"
    s3 = _cliente_s3(config_s3)
    s3.upload_fileobj(buffer, BUCKET, chave)
    uri = f"s3://{BUCKET}/{chave}"
    logger.info(f"Salvo: {uri} ({table.num_rows:,} linhas)")
    return uri


def main(full_refresh: bool = False) -> None:
    config_s3 = carregar_segredo(SEGREDO_S3)
    config_db = carregar_segredo(SEGREDO_DATABRICKS)

    ultimo_ts = None
    if not full_refresh:
        estado = ler_estado()
        if estado:
            ultimo_ts = estado["ultimo_ts"]
            logger.info(f"Extração incremental a partir de {ultimo_ts}")
        else:
            logger.info("Estado não encontrado — carga inicial completa")
    else:
        logger.info("--full-refresh: ignorando estado anterior")

    table = extrair_do_databricks(config_db, ultimo_ts)

    if table.num_rows == 0:
        logger.info("Nenhuma linha nova — nada a fazer")
        return

    ts_execucao = datetime.now(FUSO).strftime("%Y-%m-%d-%H%M%S")
    uri = salvar_no_s3(table, config_s3, ts_execucao)

    max_ts = pc.max(table.column("id_dt_atualizacao")).as_py()
    salvar_estado(max_ts, uri)
    logger.info(f"Estado atualizado: ultimo_ts = {max_ts}")
