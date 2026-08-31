import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import utils.configurar_logging as log
from utils.carregar_segredo import carregar_segredo
from utils.criar_cliente import criar_cliente

log.setup_logging()
logger = logging.getLogger(__name__)


def enviar_com_timestamp(caminho_arquivo: str, bucket_name: str, nome_segredo: str, timestamp: str):
    if not os.path.exists(caminho_arquivo):
        logger.error(log.ARQUIVO_NAO_ENCONTRADO % caminho_arquivo)
        return

    nome_base = os.path.basename(caminho_arquivo)
    nome, extensao = os.path.splitext(nome_base)

    if not timestamp:
        timestamp = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d-%H%M%S")

    novo_nome_arquivo = f"{nome}_{timestamp}{extensao}"

    logger.info(f"Iniciando upload do arquivo: {caminho_arquivo}")
    logger.info(f"Nome no bucket: {novo_nome_arquivo}")
    try:
        config = carregar_segredo(nome_segredo)
        cliente = criar_cliente(config)
        with open(caminho_arquivo, "rb") as arquivo:
            cliente.upload_fileobj(arquivo, bucket_name, novo_nome_arquivo)

        logger.info(f"Upload concluído com sucesso: {novo_nome_arquivo}")

    except Exception as e:
        logger.exception(f"Erro durante o upload: {e}")


if __name__ == "__main__":
    enviar_com_timestamp()
