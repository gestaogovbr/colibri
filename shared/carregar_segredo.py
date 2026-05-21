import os
import yaml

import shared.configurar_logging as log


_RAIZ_PROJETO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CAMINHO_SEGREDOS = os.path.join(_RAIZ_PROJETO, ".segredos.yaml")
SEGREDO_PADRAO = "colibri-token-desenvolvedor"


def carregar_segredo(
    nome_segredo: str = SEGREDO_PADRAO, caminho_arquivo: str = CAMINHO_SEGREDOS
) -> dict:
    """
    Lê o arquivo YAML de segredos e retorna o dicionário
    correspondente ao nome do segredo informado.
    """

    if not os.path.exists(caminho_arquivo):
        raise FileNotFoundError(log.ARQUIVO_NAO_ENCONTRADO % caminho_arquivo)

    with open(caminho_arquivo, "r", encoding="utf-8") as f:
        dados = yaml.safe_load(f)

    if not nome_segredo:
        raise ValueError(log.SEGREDO_VAZIO)

    if nome_segredo not in dados:
        raise KeyError(log.SEGREDO_NAO_ENCONTRADO % nome_segredo)

    return dados[nome_segredo]


if __name__ == "__main__":
    print(carregar_segredo())
