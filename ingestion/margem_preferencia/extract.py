"""
Sobe pro bucket os dados estáticos de margem de preferência.
"""

import csv
import hashlib
import logging
from datetime import datetime
from pathlib import Path

import utils.configurar_logging as log
from utils.carregar_segredo import carregar_segredo
from utils.manifesto_bucket import baixar_manifesto, subir_manifesto as _subir_manifesto
from utils.salvar_arquivo_no_bucket import salvar_arquivo_no_bucket

log.setup_logging()
logger = logging.getLogger(__name__)

NOME_SEGREDO = "colibri-token-desenvolvedor"

DIRETORIO_DADOS = Path("./dados")
DIRETORIO_MANIFESTOS = DIRETORIO_DADOS / "manifestos"
NOME_MANIFESTO = "margem_preferencia_manifesto.csv"
COLUNAS_MANIFESTO = ["arquivo", "hash_sha256", "enviado_em"]

# Caminho local (relativo a dados/) == chave no bucket
ARQUIVOS_ESTATICOS = [
    "margem_preferencia/CICS/res_1_CICS.csv",
    "margem_preferencia/CICS/res_3_CICS.csv",
    "margem_preferencia/CICS/res_4_CICS.csv",
    "margem_preferencia/CICS/res_7_CICS.csv",
    "margem_preferencia/CICS/res_8_CICS.csv",
    "margem_preferencia/CICS/resolucoes_CICS.csv",
    "margem_preferencia/CIIA-PAC/res_1_CIIA-PAC.csv",
    "margem_preferencia/CIIA-PAC/res_3_CIIA-PAC.csv",
    "margem_preferencia/CIIA-PAC/res_4_CIIA-PAC.csv",
    "margem_preferencia/CIIA-PAC/res_5_CIIA-PAC.csv",
    "margem_preferencia/CIIA-PAC/resolucoes_CIIA-PAC.csv",
    "ncm_prefixos.csv",
]


def carregar_manifesto(caminho: Path) -> dict[str, dict]:
    if not caminho.exists():
        return {}
    with open(caminho, newline="", encoding="utf-8") as f:
        return {r["arquivo"]: r for r in csv.DictReader(f)}


def salvar_manifesto(caminho: Path, manifesto: dict[str, dict]) -> None:
    with open(caminho, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUNAS_MANIFESTO)
        w.writeheader()
        w.writerows(sorted(manifesto.values(), key=lambda e: e["arquivo"]))


def _hash_arquivo(caminho: Path) -> str:
    return hashlib.sha256(caminho.read_bytes()).hexdigest()


def resetar_dados_locais() -> None:
    """Apaga o manifesto local (usado por --do-zero)"""
    (DIRETORIO_MANIFESTOS / NOME_MANIFESTO).unlink(missing_ok=True)


def subir_manifesto(bucket: str | None = None) -> None:
    """Sobe o manifesto local pro bucket. Só deve ser chamado depois do dbt rodar com sucesso"""
    bucket = bucket or carregar_segredo(NOME_SEGREDO)["bucket_lake"]
    _subir_manifesto(DIRETORIO_MANIFESTOS / NOME_MANIFESTO, NOME_MANIFESTO, bucket, NOME_SEGREDO, logger)


def executar_ingestao(bucket: str | None = None) -> bool:
    """Retorna True se algum arquivo novo/alterado foi enviado, False se nada mudou"""
    DIRETORIO_MANIFESTOS.mkdir(parents=True, exist_ok=True)
    caminho_manifesto = DIRETORIO_MANIFESTOS / NOME_MANIFESTO
    bucket = bucket or carregar_segredo(NOME_SEGREDO)["bucket_lake"]

    baixar_manifesto(caminho_manifesto, NOME_MANIFESTO, bucket, NOME_SEGREDO, logger)
    manifesto = carregar_manifesto(caminho_manifesto)

    contadores = {"enviado": 0, "ignorado": 0}
    manifesto_modificado = False

    for relativo in ARQUIVOS_ESTATICOS:
        caminho_local = DIRETORIO_DADOS / relativo
        hash_atual = _hash_arquivo(caminho_local)

        entrada = manifesto.get(relativo)
        if entrada and entrada["hash_sha256"] == hash_atual:
            logger.debug(f"{relativo}: já está no bucket, pulando")
            contadores["ignorado"] += 1
            continue

        salvar_arquivo_no_bucket(str(caminho_local), bucket, NOME_SEGREDO, relativo)
        manifesto[relativo] = {
            "arquivo": relativo,
            "hash_sha256": hash_atual,
            "enviado_em": datetime.now().isoformat(timespec="seconds"),
        }
        manifesto_modificado = True
        contadores["enviado"] += 1

    salvar_manifesto(caminho_manifesto, manifesto)
    logger.info(f"Enviados: {contadores['enviado']}  Já estavam no bucket: {contadores['ignorado']}")
    return manifesto_modificado


if __name__ == "__main__":
    executar_ingestao()
