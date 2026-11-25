"""
Extrai o cadastro de UASGs e de órgãos do Compras.gov.br (módulo UASG da API de dados abertos).

Endpoints:
  /modulo-uasg/1_consultarUasg    -> dados/uasg/uasgs_<timestamp>.csv
  /modulo-uasg/2_consultarOrgao   -> dados/uasg/orgaos_<timestamp>.csv

Os dois endpoints exigem o filtro de status (ativo/inativo). Baixamos as duas listas, para
que a inativação de uma UASG apareça no histórico como mudança de status, e não como
sumiço do registro.

A API não permite filtrar por data de alteração, então toda execução baixa o cadastro
completo (snapshot; ~100 requisições). O histórico de alterações (SCD2) é montado no dbt,
comparando os snapshots sucessivos.

Manifesto incremental (uasg_manifesto.csv):
  Uma linha por snapshot gravado: entidade, hash SHA-256 do conteúdo (registros ordenados
  pela chave, então a ordem de paginação não importa), CSV gerado, número de registros e
  timestamp. Se o hash de uma entidade for igual ao do último snapshot dela, nada é gravado.
  O manifesto é sincronizado com o bucket para persistir entre ambientes.
  O arquivo de alterações (uasg_alteracoes.csv) informa ao staging quais CSVs foram
  gerados nesta execução.

Uso:
  Executado via `ingestion.uasg.pipeline`, ou diretamente:
  python -m ingestion.uasg.extract
"""

import csv
import hashlib
import json
import logging
import random
import shutil
import time
from datetime import datetime
from pathlib import Path

import requests

import utils.configurar_logging as log
from utils.carregar_segredo import carregar_segredo
from utils.constantes import NOME_SEGREDO_DESENVOLVEDOR
from utils.manifesto_bucket import baixar_manifesto
from utils.manifesto_bucket import subir_manifesto as _subir_manifesto

log.setup_logging()
logger = logging.getLogger(__name__)


# Constantes

BASE_URL = "https://dadosabertos.compras.gov.br"
MAX_RETRIES = 5
BASE_BACKOFF = 2.0
TIMEOUT = 120

# Mesmo limite medido no módulo de material (~100 req/min, 429 a partir de 2 req/s).
# As requisições aqui são sequenciais; o intervalo mínimo entre elas evita o 429.
PACING_INTERVALO = 0.7

# Quantas vezes refazer a extração de uma entidade quando a paginação devolve menos
# registros distintos que o total informado pela API (cadastro mudando durante a leitura).
MAX_TENTATIVAS_SNAPSHOT = 3

DIRETORIO_RAIZ = Path("./dados")
DIRETORIO_SAIDA = DIRETORIO_RAIZ / "uasg"
DIRETORIO_MANIFESTOS = DIRETORIO_RAIZ / "manifestos"
DIRETORIO_ALTERACOES = DIRETORIO_RAIZ / "alteracoes"

NOME_MANIFESTO = "uasg_manifesto.csv"
COLUNAS_MANIFESTO = ["entidade", "hash_sha256", "arquivo_csv", "num_registros", "extraido_em"]

NOME_ALTERACOES = "uasg_alteracoes.csv"
COLUNAS_ALTERACOES = ["entidade", "arquivo_csv"]

# Coluna acrescentada a cada linha do CSV: o momento da extração, usado pelo dbt como
# início de vigência da versão (valido_de).
COLUNA_EXTRAIDO_EM = "extraido_em"

ENTIDADES = {
    "uasgs": {
        "endpoint": "/modulo-uasg/1_consultarUasg",
        "param_status": "statusUasg",
        "chave": "codigoUasg",
        "campos": [
            "codigoUasg",
            "nomeUasg",
            "usoSisg",
            "adesaoSiasg",
            "siglaUf",
            "codigoMunicipio",
            "codigoMunicipioIbge",
            "nomeMunicipioIbge",
            "codigoUnidadePolo",
            "nomeUnidadePolo",
            "codigoUnidadeEspelho",
            "nomeUnidadeEspelho",
            "uasgCadastradora",
            "cnpjCpfUasg",
            "codigoOrgao",
            "cnpjCpfOrgao",
            "cnpjCpfOrgaoVinculado",
            "cnpjCpfOrgaoSuperior",
            "codigoSiorg",
            "statusUasg",
            "dataImplantacaoSidec",
            "dataHoraMovimento",
        ],
    },
    "orgaos": {
        "endpoint": "/modulo-uasg/2_consultarOrgao",
        "param_status": "statusOrgao",
        "chave": "codigoOrgao",
        "campos": [
            "codigoOrgao",
            "nomeOrgao",
            "nomeMnemonicoOrgao",
            "cnpjCpfOrgao",
            "codigoOrgaoVinculado",
            "cnpjCpfOrgaoVinculado",
            "nomeOrgaoVinculado",
            "codigoOrgaoSuperior",
            "cnpjCpfOrgaoSuperior",
            "nomeOrgaoSuperior",
            "codigoTipoAdministracao",
            "nomeTipoAdministracao",
            "poder",
            "esfera",
            "usoSisg",
            "statusOrgao",
            "dataHoraMovimento",
        ],
    },
}


# Manifesto e alterações


def carregar_manifesto(caminho: Path) -> list[dict]:
    if not caminho.exists():
        return []
    with open(caminho, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def salvar_manifesto(caminho: Path, manifesto: list[dict]) -> None:
    with open(caminho, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUNAS_MANIFESTO)
        writer.writeheader()
        writer.writerows(manifesto)


def salvar_alteracoes(caminho: Path, alteracoes: list[tuple[str, str]]) -> None:
    with open(caminho, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(COLUNAS_ALTERACOES)
        writer.writerows(alteracoes)


def ultimo_hash(manifesto: list[dict], entidade: str) -> str | None:
    entradas = [e for e in manifesto if e["entidade"] == entidade]
    if not entradas:
        return None
    return max(entradas, key=lambda e: e["extraido_em"])["hash_sha256"]


# API

_proxima_permitida: float = 0.0


def _aguardar_pacing() -> None:
    global _proxima_permitida
    agora = time.monotonic()
    if agora < _proxima_permitida:
        time.sleep(_proxima_permitida - agora)
    _proxima_permitida = time.monotonic() + PACING_INTERVALO


def _buscar_pagina(session: requests.Session, endpoint: str, params: dict) -> dict:
    for tentativa in range(MAX_RETRIES):
        _aguardar_pacing()
        try:
            resp = session.get(f"{BASE_URL}{endpoint}", params=params, timeout=TIMEOUT)
            if resp.status_code == 429:
                espera = float(resp.headers.get("Retry-After", BASE_BACKOFF * (2**tentativa)))
                logger.warning(f"429 em {endpoint} {params}. Aguardando {espera:.1f}s")
                time.sleep(espera)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if tentativa == MAX_RETRIES - 1:
                raise RuntimeError(f"Falha definitiva em {endpoint} {params}: {e}") from e
            time.sleep(BASE_BACKOFF * (2**tentativa) + random.uniform(0, 0.5))
    raise RuntimeError(f"Falha definitiva em {endpoint} {params}: excesso de 429")


def _extrair_uma_vez(session: requests.Session, cfg: dict) -> tuple[list[dict], int]:
    """Baixa todas as páginas de ativos e inativos. Retorna (registros, total informado pela API)"""
    registros: list[dict] = []
    total_api = 0
    for status in ("true", "false"):
        params = {"pagina": 1, cfg["param_status"]: status}
        primeira = _buscar_pagina(session, cfg["endpoint"], params)
        total_api += primeira["totalRegistros"]
        registros.extend(primeira.get("resultado") or [])
        for pagina in range(2, primeira["totalPaginas"] + 1):
            dados = _buscar_pagina(session, cfg["endpoint"], {**params, "pagina": pagina})
            registros.extend(dados.get("resultado") or [])
    return registros, total_api


def deduplicar(registros: list[dict], chave: str) -> list[dict]:
    """Uma linha por chave. Se a paginação repetir a chave com conteúdo diferente, fica a
    versão com dataHoraMovimento mais recente"""
    por_chave: dict = {}
    for r in registros:
        k = r.get(chave)
        atual = por_chave.get(k)
        if atual is None or (r.get("dataHoraMovimento") or "") > (atual.get("dataHoraMovimento") or ""):
            por_chave[k] = r
    return list(por_chave.values())


def normalizar(registros: list[dict], campos: list[str], chave: str) -> list[dict]:
    """Mantém só os campos esperados, na ordem do CSV, ordenando pela chave (como texto)"""
    linhas = [{c: r.get(c) for c in campos} for r in registros]
    return sorted(linhas, key=lambda r: str(r[chave]))


def calcular_hash(linhas: list[dict]) -> str:
    conteudo = json.dumps(linhas, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(conteudo.encode()).hexdigest()


def extrair_entidade(session: requests.Session, entidade: str) -> list[dict]:
    cfg = ENTIDADES[entidade]
    for tentativa in range(1, MAX_TENTATIVAS_SNAPSHOT + 1):
        brutos, total_api = _extrair_uma_vez(session, cfg)
        unicos = deduplicar(brutos, cfg["chave"])
        logger.info(
            f"{entidade}: {len(brutos):,} registro(s) lidos, {len(unicos):,} distintos, "
            f"{total_api:,} informados pela API (tentativa {tentativa})"
        )
        # Menos chaves que o total informado = paginação perdeu registros. Gravar esse
        # snapshot criaria exclusões falsas no histórico, então tentamos de novo ou falhamos.
        if len(unicos) >= total_api:
            return normalizar(unicos, cfg["campos"], cfg["chave"])
        logger.warning(f"{entidade}: snapshot incompleto ({len(unicos):,} de {total_api:,}).")
    raise RuntimeError(f"{entidade}: snapshot incompleto após {MAX_TENTATIVAS_SNAPSHOT} tentativas")


# CSV


def salvar_csv(entidade: str, linhas: list[dict], agora: datetime) -> Path:
    DIRETORIO_SAIDA.mkdir(parents=True, exist_ok=True)
    caminho = DIRETORIO_SAIDA / f"{entidade}_{agora.strftime('%Y-%m-%d-%H%M%S')}.csv"
    extraido_em = agora.isoformat(sep=" ", timespec="seconds")
    campos = ENTIDADES[entidade]["campos"] + [COLUNA_EXTRAIDO_EM]
    with open(caminho, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=campos)
        writer.writeheader()
        writer.writerows({**linha, COLUNA_EXTRAIDO_EM: extraido_em} for linha in linhas)
    logger.info(f"Snapshot gravado: {caminho} ({len(linhas):,} linhas)")
    return caminho


# Execução


def resetar_dados_locais() -> None:
    """Apaga os CSVs extraídos, o manifesto e as alterações locais"""
    shutil.rmtree(DIRETORIO_SAIDA, ignore_errors=True)
    (DIRETORIO_MANIFESTOS / NOME_MANIFESTO).unlink(missing_ok=True)
    (DIRETORIO_ALTERACOES / NOME_ALTERACOES).unlink(missing_ok=True)


def subir_manifesto(bucket: str | None = None) -> None:
    """Sobe o manifesto local pro bucket. Só deve ser chamado depois do dbt rodar com sucesso"""
    bucket = bucket or carregar_segredo(NOME_SEGREDO_DESENVOLVEDOR)["bucket_lake"]
    _subir_manifesto(
        DIRETORIO_MANIFESTOS / NOME_MANIFESTO,
        NOME_MANIFESTO,
        bucket,
        NOME_SEGREDO_DESENVOLVEDOR,
        logger,
    )


def executar_ingestao(bucket: str | None = None, sincronizar_manifesto: bool = True) -> bool:
    """Retorna True se algum snapshot novo foi gravado, False se nada mudou.

    sincronizar_manifesto=False usa só o manifesto local (útil para rodar sem credenciais)"""
    DIRETORIO_SAIDA.mkdir(parents=True, exist_ok=True)
    DIRETORIO_MANIFESTOS.mkdir(parents=True, exist_ok=True)
    DIRETORIO_ALTERACOES.mkdir(parents=True, exist_ok=True)
    caminho_manifesto = DIRETORIO_MANIFESTOS / NOME_MANIFESTO
    caminho_alteracoes = DIRETORIO_ALTERACOES / NOME_ALTERACOES

    caminho_alteracoes.unlink(missing_ok=True)

    if sincronizar_manifesto:
        bucket = bucket or carregar_segredo(NOME_SEGREDO_DESENVOLVEDOR)["bucket_lake"]
        baixar_manifesto(caminho_manifesto, NOME_MANIFESTO, bucket, NOME_SEGREDO_DESENVOLVEDOR, logger)
    manifesto = carregar_manifesto(caminho_manifesto)

    alteracoes: list[tuple[str, str]] = []
    with requests.Session() as session:
        session.headers["User-Agent"] = "colibri-ingestao/1.0"
        for entidade in ENTIDADES:
            linhas = extrair_entidade(session, entidade)
            hash_atual = calcular_hash(linhas)
            if hash_atual == ultimo_hash(manifesto, entidade):
                logger.info(f"{entidade}: sem mudanças desde o último snapshot.")
                continue

            agora = datetime.now()
            caminho_csv = salvar_csv(entidade, linhas, agora)
            manifesto.append(
                {
                    "entidade": entidade,
                    "hash_sha256": hash_atual,
                    "arquivo_csv": caminho_csv.name,
                    "num_registros": len(linhas),
                    "extraido_em": agora.isoformat(timespec="seconds"),
                }
            )
            alteracoes.append((entidade, caminho_csv.name))

    salvar_manifesto(caminho_manifesto, manifesto)
    salvar_alteracoes(caminho_alteracoes, alteracoes)
    logger.info(f"Manifesto: {caminho_manifesto} ({len(manifesto)} entrada(s))")
    logger.info(f"Alterações: {caminho_alteracoes} ({len(alteracoes)} arquivo(s))")
    return bool(alteracoes)


if __name__ == "__main__":
    executar_ingestao()
