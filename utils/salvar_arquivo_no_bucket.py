import os
import logging
import utils.configurar_logging as log

from utils.carregar_segredo import carregar_segredo
from utils.criar_cliente import criar_cliente


log.setup_logging()
logger = logging.getLogger(__name__)


def salvar_arquivo_no_bucket(
    caminho_arquivo: str,
    bucket_name: str,
    nome_segredo: str,
    nome_no_bucket: str | None = None,
) -> None:
    if not os.path.exists(caminho_arquivo):
        logger.error(log.ARQUIVO_NAO_ENCONTRADO % caminho_arquivo)
        return

    nome_destino = nome_no_bucket or os.path.basename(caminho_arquivo)

    config = carregar_segredo(nome_segredo)
    cliente = criar_cliente(config)
    with open(caminho_arquivo, "rb") as arquivo:
        cliente.upload_fileobj(arquivo, bucket_name, nome_destino)
    logger.info(f"Upload concluído: {nome_destino}")


if __name__ == "__main__":
    salvar_arquivo_no_bucket()
