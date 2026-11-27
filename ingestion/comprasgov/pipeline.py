"""Orquestra a ingestão incremental dos dados PNCP do ComprasGOV."""

import hashlib
import logging
from pathlib import Path

import requests

from ingestion.comprasgov.delta import gerar_deltas_parquet
from ingestion.comprasgov.extract import (
    DIRETORIO_SAIDA,
    processar_tipo_detalhado,
)
from ingestion.pncp_comprasgov.lock_delog import bloquear_destino
from utils.manifesto_duckdb import (
    consultar_aplicacao_pendente,
    consultar_entrada,
    consultar_versoes,
    criar_aplicacao,
    inicializar_manifesto,
    registrar_deltas,
)

LOGGER = logging.getLogger(__name__)

PIPELINE = "comprasgov_pncp"

CHAVES = {
    "compras": ["cod_compra"],
    "itens": ["cod_item_compra", "COD_RESULTADO_ITEM"],
    "resultados": ["id_compra_item", "sequencial_resultado"],
}


def localizar_versao(
    tipo: str,
    ano: int,
    mes: int,
    hash_sha256: str,
) -> dict:
    """Localiza uma versão específica do snapshot."""

    versoes = consultar_versoes(PIPELINE, tipo, ano, mes)

    for versao in versoes:
        if versao["hash_sha256"] == hash_sha256:
            return versao

    raise ValueError(f"Versão não encontrada: {tipo}/{ano}/{mes:02d}/{hash_sha256}")


def validar_snapshot(versao: dict) -> Path:
    """Verifica a existência e integridade do snapshot."""

    caminho = Path(versao["caminho_snapshot"])

    if not caminho.is_file():
        raise FileNotFoundError(f"Snapshot não encontrado: {caminho}")

    with caminho.open("rb") as arquivo:
        hash_sha256 = hashlib.file_digest(arquivo, "sha256").hexdigest()

    if hash_sha256 != versao["hash_sha256"]:
        raise ValueError(f"Snapshot corrompido: {caminho}")

    return caminho


def preparar_aplicacao(
    sessao: requests.Session,
    ano: int,
    mes: int,
    tipo: str,
) -> dict:
    """Prepara ou recupera uma aplicação incremental."""

    if tipo not in CHAVES:
        raise ValueError(f"Tipo inválido: {tipo}")

    if not 1 <= mes <= 12:
        raise ValueError(f"Mês inválido: {mes}")

    with bloquear_destino(DIRETORIO_SAIDA / PIPELINE):
        inicializar_manifesto(PIPELINE)

        pendente = consultar_aplicacao_pendente(PIPELINE, tipo, ano, mes)

        if pendente is not None:
            return gerar_deltas_aplicacao(tipo, ano, mes, pendente)

        return preparar_nova_aplicacao(sessao, ano, mes, tipo)


def gerar_deltas_aplicacao(
    tipo: str,
    ano: int,
    mes: int,
    aplicacao: dict,
) -> dict:
    """Gera ou recupera os deltas de uma aplicação."""

    id_aplicacao = aplicacao["id_aplicacao"]

    baseline = consultar_entrada(PIPELINE, tipo, ano, mes)

    hash_atual = baseline["hash_sha256"] if baseline is not None else None

    if hash_atual != aplicacao["hash_anterior"]:
        raise ValueError("Baseline divergente da aplicação pendente")

    versao_nova = localizar_versao(tipo, ano, mes, aplicacao["hash_novo"])

    caminho_novo = validar_snapshot(versao_nova)
    caminho_anterior = validar_snapshot(baseline) if baseline is not None else None

    if aplicacao["status"] == "deltas_gerados":
        destino = Path(aplicacao["caminho_deltas"])

        for operacao in ("insert", "update", "delete"):
            arquivo = destino / f"{operacao}.parquet"
            if not arquivo.is_file():
                raise FileNotFoundError(arquivo)

        return {
            "status": "preparado",
            "id_aplicacao": id_aplicacao,
            "caminho_deltas": str(destino),
        }

    if aplicacao["status"] != "pendente":
        raise ValueError(f"Status inesperado: {aplicacao['status']}")

    destino = DIRETORIO_SAIDA / tipo / str(ano) / f"{mes:02d}" / "deltas" / f"aplicacao_{id_aplicacao:06d}"

    if destino.exists():
        raise RuntimeError(f"Deltas já publicados sem registro: {destino}")

    resultado = gerar_deltas_parquet(
        baseline=caminho_anterior,
        novo=caminho_novo,
        pasta_destino=destino,
        prefixo_versao=f"aplicacao_{id_aplicacao:06d}",
        chaves=CHAVES[tipo],
    )

    registrar_deltas(
        PIPELINE,
        id_aplicacao,
        str(destino),
        resultado["insert"],
        resultado["update"],
        resultado["delete"],
    )

    LOGGER.info(
        "Aplicação %s preparada: %s",
        id_aplicacao,
        resultado,
    )

    return {
        "status": "preparado",
        "id_aplicacao": id_aplicacao,
        "caminho_deltas": str(destino),
        **resultado,
    }


def preparar_nova_aplicacao(
    sessao: requests.Session,
    ano: int,
    mes: int,
    tipo: str,
) -> dict:
    """Extrai e registra uma nova aplicação."""

    resultado = processar_tipo_detalhado(sessao, ano, mes, tipo)

    if resultado["status"] != "criado":
        return resultado

    baseline = consultar_entrada(PIPELINE, tipo, ano, mes)

    hash_anterior = baseline["hash_sha256"] if baseline is not None else None

    hash_novo = resultado["hash_sha256"]

    if baseline is not None:
        if not hash_anterior:
            raise ValueError("Baseline sem hash registrado")

        validar_snapshot(baseline)

    validar_snapshot(resultado)

    if hash_anterior == hash_novo:
        return {"status": "existente"}

    criar_aplicacao(
        PIPELINE,
        tipo,
        ano,
        mes,
        hash_anterior,
        hash_novo,
    )

    pendente = consultar_aplicacao_pendente(PIPELINE, tipo, ano, mes)

    if pendente is None:
        raise RuntimeError("Aplicação criada não encontrada")

    return gerar_deltas_aplicacao(tipo, ano, mes, pendente)
