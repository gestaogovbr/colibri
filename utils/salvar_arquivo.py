import boto3
import click
import os
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from utils.carregar_segredo import carregar_segredo
import utils.configurar_logging as log


log.setup_logging()
logger = logging.getLogger(__name__)


def carregar_config_bucket(nome_segredo: str):
    config = carregar_segredo(nome_segredo)

    try:
        return (
            config["endpoint"],
            config["access_key"],
            config["secret_key"],
        )
    except KeyError as e:
        logger.error(log.SEGREDO_CAMPO_AUSENTE % (nome_segredo, e))
        raise


def enviar_com_timestamp(
    caminho_arquivo: str, bucket_name: str, nome_segredo: str, timestamp: str
):
    if not os.path.exists(caminho_arquivo):
        logger.error(log.ARQUIVO_NAO_ENCONTRADO % caminho_arquivo)
        return

    endpoint, access_key, secret_key = carregar_config_bucket(nome_segredo)

    nome_base = os.path.basename(caminho_arquivo)
    nome, extensao = os.path.splitext(nome_base)

    if not timestamp:
        timestamp = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime(
            "%Y-%m-%d-%H%M%S"
        )

    novo_nome_arquivo = f"{nome}_{timestamp}{extensao}"

    logger.info(f"Iniciando upload do arquivo: {caminho_arquivo}")
    logger.info(f"Nome no bucket: {novo_nome_arquivo}")
    try:
        cliente_s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="auto",
        )

        with open(caminho_arquivo, "rb") as arquivo:
            cliente_s3.upload_fileobj(arquivo, bucket_name, novo_nome_arquivo)

        logger.info(f"Upload concluído com sucesso: {novo_nome_arquivo}")

    except Exception as e:
        logger.exception(f"Erro durante o upload: {e}")


@click.command()
@click.argument("caminho_arquivo")
@click.argument("bucket_name")
@click.argument("nome_segredo")
@click.option(
    "--timestamp",
    required=False,
    help="Timestamp opcional (ex: 2026-05-04T10:00:00)",
)
def enviar_com_timestamp_a_partir_do_terminal(
    caminho_arquivo: str, bucket_name: str, nome_segredo: str, timestamp: str | None
):
    enviar_com_timestamp(caminho_arquivo, bucket_name, nome_segredo, timestamp)


if __name__ == "__main__":
    enviar_com_timestamp_a_partir_do_terminal()
