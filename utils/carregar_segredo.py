import os

import yaml

import utils.configurar_logging as log
from utils.constantes import CAMINHO_SEGREDOS, NOME_SEGREDO_DESENVOLVEDOR


def carregar_segredo(
    nome_segredo: str = NOME_SEGREDO_DESENVOLVEDOR,
    caminho_arquivo: str = CAMINHO_SEGREDOS,
) -> dict:
    """
    Lê o arquivo YAML de segredos e retorna o dicionário correspondente ao nome do segredo informado
    """

    if not os.path.exists(caminho_arquivo):
        raise FileNotFoundError(log.ARQUIVO_NAO_ENCONTRADO % caminho_arquivo)

    with open(caminho_arquivo, encoding="utf-8") as f:
        dados = yaml.safe_load(f)

    if not nome_segredo:
        raise ValueError(log.SEGREDO_VAZIO)

    if nome_segredo not in dados:
        raise KeyError(log.SEGREDO_NAO_ENCONTRADO % nome_segredo)

    # Ex: dados -> {
    #                 "colibri-token-desenvolvedor": {
    #                     "endpoint": "https://<ACCOUNTID>.r2.cloudflarestorage.com",
    #                     "access_key": "<SUA ACCESS KEY>",
    #                     "secret_key": "<SUA SECRET KEY>",
    #                 },
    #                 ...
    #              }

    return dados[nome_segredo]


if __name__ == "__main__":
    print(carregar_segredo())
