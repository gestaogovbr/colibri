"""Boilerplate comum de manifesto compartilhado entre os extractors via bucket"""

import logging
from pathlib import Path

from utils.baixar_arquivo import baixar_arquivo_do_bucket
from utils.salvar_arquivo_no_bucket import salvar_arquivo_no_bucket


def baixar_manifesto(
    caminho_local: Path,
    nome_manifesto: str,
    bucket: str,
    segredo: str,
    logger: logging.Logger,
    apagar_antes: bool = True,
) -> None:
    """Baixa o manifesto do bucket pra caminho_local. Se apagar_antes=True (padrão),
    remove a cópia local antes de tentar -- garante que uma falha no download não
    deixe um manifesto antigo/desatualizado sendo usado por engano"""
    if apagar_antes:
        caminho_local.unlink(missing_ok=True)
    try:
        baixar_arquivo_do_bucket(nome_manifesto, bucket, segredo, str(caminho_local))
        logger.info(f"Manifesto baixado do bucket: {bucket}/{nome_manifesto}")
    except Exception as e:
        logger.warning(f"Manifesto não encontrado no bucket, iniciando do zero: {e}")


def subir_manifesto(
    caminho_local: Path,
    nome_manifesto: str,
    bucket: str,
    segredo: str,
    logger: logging.Logger,
) -> None:
    """Sobe o manifesto local pro bucket, se ele existir. Só deve ser chamado
    depois do dbt rodar com sucesso (ou de confirmar que não havia nada a
    rodar) -- ver pipeline.py de cada módulo"""
    if not caminho_local.exists():
        return
    try:
        salvar_arquivo_no_bucket(str(caminho_local), bucket, segredo, nome_manifesto)
    except Exception as e:
        logger.warning(f"Não foi possível salvar manifesto no bucket: {e}")
