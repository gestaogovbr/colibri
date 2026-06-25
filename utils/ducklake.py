import os
from urllib.parse import urlparse

import boto3
import duckdb
from botocore.exceptions import ClientError

from utils.carregar_segredo import carregar_segredo


CATALOGO_LOCAL = "meta.ducklake"
_SESSION_DB = "ducklake_session.duckdb"


def _cliente_s3(config: dict):
    return boto3.client(
        "s3",
        endpoint_url=config["endpoint"],
        aws_access_key_id=config["access_key"],
        aws_secret_access_key=config["secret_key"],
        region_name="auto",
    )


def _parsear_s3(caminho_meta: str) -> tuple[str, str]:
    parsed = urlparse(caminho_meta)
    return parsed.netloc, parsed.path.lstrip("/")


def _baixar_catalogo(cliente, bucket: str, chave: str):
    try:
        cliente.download_file(bucket, chave, CATALOGO_LOCAL)
        print(f"[ducklake] Catálogo baixado de s3://{bucket}/{chave}")
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            print("[ducklake] Catálogo não existe no R2. Será criado.")
            if os.path.exists(CATALOGO_LOCAL):
                os.remove(CATALOGO_LOCAL)
        else:
            raise


def _subir_catalogo(cliente, bucket: str, chave: str):
    cliente.upload_file(CATALOGO_LOCAL, bucket, chave)
    print(f"[ducklake] Catálogo sincronizado para s3://{bucket}/{chave}")


def _checkpoint_catalogo():
    try:
        con = duckdb.connect(CATALOGO_LOCAL)
        con.execute("FORCE CHECKPOINT")
        con.close()
    except Exception as e:
        print(f"[ducklake] Aviso: checkpoint falhou: {e}")


def _nova_conexao(config: dict) -> duckdb.DuckDBPyConnection:
    endpoint = urlparse(config["endpoint"]).netloc
    if os.path.exists(_SESSION_DB):
        os.remove(_SESSION_DB)
    con = duckdb.connect(_SESSION_DB)
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    con.execute("INSTALL ducklake")
    con.execute("LOAD ducklake")
    con.execute(f"SET s3_endpoint = '{endpoint}'")
    con.execute(f"SET s3_access_key_id = '{config['access_key']}'")
    con.execute(f"SET s3_secret_access_key = '{config['secret_key']}'")
    con.execute("SET s3_region = 'auto'")
    con.execute("SET s3_url_style = 'path'")
    return con


def conectar(caminho_meta: str, data_path: str, nome_segredo: str) -> duckdb.DuckDBPyConnection:
    config = carregar_segredo(nome_segredo)
    cliente = _cliente_s3(config)
    bucket, chave = _parsear_s3(caminho_meta)
    _baixar_catalogo(cliente, bucket, chave)

    con = _nova_conexao(config)
    con.execute(f"ATTACH 'ducklake:{CATALOGO_LOCAL}' AS lake (DATA_PATH '{data_path}')")
    return con


def fechar(con: duckdb.DuckDBPyConnection, caminho_meta: str, nome_segredo: str):
    con.execute("DETACH lake")
    con.close()
    if os.path.exists(_SESSION_DB):
        os.remove(_SESSION_DB)
    _checkpoint_catalogo()
    config = carregar_segredo(nome_segredo)
    cliente = _cliente_s3(config)
    bucket, chave = _parsear_s3(caminho_meta)
    _subir_catalogo(cliente, bucket, chave)
