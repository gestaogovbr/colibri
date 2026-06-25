import logging
import requests

import utils.configurar_logging as log

log.setup_logging()
logger = logging.getLogger(__name__)


def carregar_json_da_url(url: str, timeout: int = 10):
    """
    Busca dados JSON de uma URL e os retorna como um objeto Python
    """

    logger.info("Buscando JSON de %s", url)

    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()  # Erros HTTP (4xx, 5xx)

    except requests.exceptions.Timeout:
        logger.error(log.REQUISICAO_EXPIRADA, url, timeout)
        raise

    except requests.exceptions.HTTPError as e:
        logger.error(log.ERRO_HTTP, url, e)
        raise

    except requests.exceptions.RequestException as e:
        logger.error(log.REQUISICAO_FALHOU, url, e)
        raise

    try:
        data = response.json()

    except ValueError:
        logger.error(log.JSON_INVALIDO, url)
        raise

    logger.info(log.JSON_SUCESSO, url)

    return data
