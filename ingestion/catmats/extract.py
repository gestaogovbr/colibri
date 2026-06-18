"""
Extrai o catálogo completo de itens CATMAT via API do ComprasGOV.

Pagina /modulo-material/4_consultarItemMaterial e salva em dados/catmats/catmats_api.csv.

Manifesto incremental (catmats_manifesto.csv):
  Registra hash SHA-256 da resposta bruta por página. Páginas com hash inalterado
  são puladas. O manifesto é sincronizado com o bucket para persistir entre ambientes.

Uso:
  python -m ingestion.catmats.extract
"""

import csv
import hashlib
import logging
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import requests
from tqdm import tqdm

import shared.configurar_logging as log
from shared.baixar_arquivo import baixar_arquivo_do_bucket
from shared.carregar_segredo import carregar_segredo
from shared.salvar_arquivo import salvar_arquivo_no_bucket

log.setup_logging()
logger = logging.getLogger(__name__)

BASE_URL = "https://dadosabertos.compras.gov.br"
ENDPOINT = "/modulo-material/4_consultarItemMaterial"
TAMANHO_PAGINA = 100
MAX_WORKERS = 3
CHUNK_SIZE = 15
PAUSA = 0.5
BATCH_SIZE = 200
MAX_RETRIES = 5
BASE_BACKOFF = 2.0

# Limite real da API (medido empiricamente): ~100 req/min (429 a partir de 2 req/s).
# Pacing global mantém todas as threads abaixo disso, evitando 429 em vez de só reagir.
PACING_INTERVALO = 0.7

# Rate limit global: quando qualquer thread bate 429, todas aguardam
_rl_lock = threading.Lock()
_rl_ate: float = 0.0
_proxima_permitida: float = 0.0


def _aguardar_rate_limit() -> None:
    global _proxima_permitida
    with _rl_lock:
        agora = time.monotonic()
        inicio = max(agora, _rl_ate, _proxima_permitida)
        _proxima_permitida = inicio + PACING_INTERVALO
        espera = inicio - agora
    if espera > 0:
        time.sleep(espera)


def _registrar_rate_limit(espera: float) -> None:
    global _rl_ate
    with _rl_lock:
        _rl_ate = max(_rl_ate, time.monotonic() + espera)
SEGREDO_NOME = "colibri-token-desenvolvedor"

DIRETORIO_DADOS = Path("./dados")
DIRETORIO_CATMATS = DIRETORIO_DADOS / "catmats"
DIRETORIO_MANIFESTOS = DIRETORIO_DADOS / "manifestos"
CAMINHO_CSV = DIRETORIO_CATMATS / "catmats_api.csv"
NOME_MANIFESTO = "catmats_manifesto.csv"

COLUNAS_CSV = [
    "codigoGrupo", "nomeGrupo", "codigoClasse", "nomeClasse",
    "codigoPdm", "nomePdm", "codigoItem", "descricaoItem",
    "statusItem", "itemSustentavel", "codigo_ncm", "descricao_ncm",
    "aplica_margem_preferencia", "dataHoraAtualizacao", "api_consultada_em",
]
CAMPOS_API = COLUNAS_CSV[:-1]
COLUNAS_MANIFESTO = ["pagina", "hash_sha256", "num_registros", "consultada_em"]


def carregar_manifesto(caminho: Path) -> dict[int, dict]:
    if not caminho.exists():
        return {}
    with open(caminho, newline="", encoding="utf-8") as f:
        return {int(r["pagina"]): r for r in csv.DictReader(f)}


def salvar_manifesto(caminho: Path, manifesto: dict[int, dict]) -> None:
    with open(caminho, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUNAS_MANIFESTO)
        w.writeheader()
        w.writerows(sorted(manifesto.values(), key=lambda e: int(e["pagina"])))


def _buscar_pagina(pagina: int, session: requests.Session) -> dict:
    params = {"pagina": pagina, "tamanhoPagina": TAMANHO_PAGINA}

    for tentativa in range(MAX_RETRIES):
        _aguardar_rate_limit()
        try:
            resp = session.get(f"{BASE_URL}{ENDPOINT}", params=params, timeout=30)

            if resp.status_code == 429:
                espera = float(resp.headers.get("Retry-After", BASE_BACKOFF * (2 ** tentativa)))
                logger.warning(f"429 na página {pagina}. Pausa global de {espera:.1f}s")
                _registrar_rate_limit(espera)
                time.sleep(espera)
                continue

            resp.raise_for_status()
            conteudo_bytes = resp.content
            hash_atual = hashlib.sha256(conteudo_bytes).hexdigest()
            timestamp = datetime.now().isoformat(timespec="seconds")
            registros = [
                [item.get(c) for c in CAMPOS_API] + [timestamp]
                for item in resp.json().get("resultado", [])
            ]
            return {"pagina": pagina, "registros": registros, "hash": hash_atual, "ok": True}

        except requests.RequestException as e:
            if tentativa == MAX_RETRIES - 1:
                logger.error(f"Falha definitiva na página {pagina}: {e}")
                return {"pagina": pagina, "registros": [], "hash": None, "ok": False}
            time.sleep(BASE_BACKOFF * (2 ** tentativa) + random.uniform(0, 0.5))

    return {"pagina": pagina, "registros": [], "hash": None, "ok": False}


def executar_ingestao() -> None:
    DIRETORIO_CATMATS.mkdir(parents=True, exist_ok=True)
    DIRETORIO_MANIFESTOS.mkdir(parents=True, exist_ok=True)

    caminho_manifesto = DIRETORIO_MANIFESTOS / NOME_MANIFESTO
    bucket = carregar_segredo(SEGREDO_NOME)["bucket_lake"]

    caminho_manifesto.unlink(missing_ok=True)
    try:
        baixar_arquivo_do_bucket(NOME_MANIFESTO, bucket, SEGREDO_NOME, str(caminho_manifesto))
        logger.info(f"Manifesto baixado do bucket: {bucket}/{NOME_MANIFESTO}")
    except Exception as e:
        logger.warning(f"Manifesto não encontrado no bucket, iniciando do zero: {e}")

    manifesto = carregar_manifesto(caminho_manifesto)

    logger.info("Consultando total de páginas do catálogo CATMAT...")
    with requests.Session() as s:
        s.headers["User-Agent"] = "colibri-ingestao/1.0"
        for tentativa in range(MAX_RETRIES):
            try:
                resp = s.get(
                    f"{BASE_URL}{ENDPOINT}",
                    params={"pagina": 1, "tamanhoPagina": TAMANHO_PAGINA},
                    timeout=30,
                )
                resp.raise_for_status()
                meta = resp.json()
                break
            except requests.RequestException as e:
                if tentativa == MAX_RETRIES - 1:
                    raise RuntimeError(f"Falha ao consultar metadados: {e}")
                time.sleep(BASE_BACKOFF * (2 ** tentativa))

    total_paginas = meta["totalPaginas"]
    logger.info(f"Total: {meta['totalRegistros']:,} registros | {total_paginas:,} páginas")

    todas_paginas = list(range(1, total_paginas + 1))
    pendentes = [p for p in todas_paginas if p not in manifesto]
    logger.info(f"Pendentes: {len(pendentes):,} | Já no manifesto: {len(manifesto):,}")

    if not pendentes:
        logger.info("Catálogo já atualizado.")
        return

    csv_novo = not CAMINHO_CSV.exists()
    buf_registros: list[list] = []
    contadores = {"baixado": 0, "atualizado": 0, "ignorado": 0}
    manifesto_modificado = False

    def _flush() -> None:
        nonlocal csv_novo
        if buf_registros:
            with open(CAMINHO_CSV, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                if csv_novo:
                    w.writerow(COLUNAS_CSV)
                    csv_novo = False
                w.writerows(buf_registros)
            buf_registros.clear()

    chunks = [pendentes[i:i + CHUNK_SIZE] for i in range(0, len(pendentes), CHUNK_SIZE)]

    try:
        with tqdm(total=len(pendentes), unit="pág", desc="CATMAT") as barra, \
             requests.Session() as session:
            session.headers["User-Agent"] = "colibri-ingestao/1.0"
            for idx, chunk in enumerate(chunks):
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = {executor.submit(_buscar_pagina, p, session): p for p in chunk}
                    for future in as_completed(futures):
                        resultado = future.result()
                        pagina = resultado["pagina"]

                        if not resultado["ok"]:
                            barra.update(1)
                            continue

                        entrada = manifesto.get(pagina)
                        if entrada and entrada["hash_sha256"] == resultado["hash"]:
                            contadores["ignorado"] += 1
                            barra.update(1)
                            continue

                        buf_registros.extend(resultado["registros"])
                        status = "atualizado" if entrada else "baixado"
                        contadores[status] += 1
                        manifesto[pagina] = {
                            "pagina": pagina,
                            "hash_sha256": resultado["hash"],
                            "num_registros": len(resultado["registros"]),
                            "consultada_em": datetime.now().isoformat(timespec="seconds"),
                        }
                        manifesto_modificado = True
                        barra.update(1)

                if len(buf_registros) >= BATCH_SIZE:
                    _flush()

                if idx < len(chunks) - 1:
                    time.sleep(PAUSA)

    except KeyboardInterrupt:
        logger.warning("Interrompido pelo usuário.")
    finally:
        _flush()
        salvar_manifesto(caminho_manifesto, manifesto)
        logger.info(
            f"Concluído.  "
            f"Baixados: {contadores['baixado']}  "
            f"Atualizados: {contadores['atualizado']}  "
            f"Ignorados: {contadores['ignorado']}"
        )
        if manifesto_modificado:
            try:
                salvar_arquivo_no_bucket(str(caminho_manifesto), bucket, SEGREDO_NOME, NOME_MANIFESTO)
            except Exception as e:
                logger.warning(f"Não foi possível salvar manifesto no bucket: {e}")


if __name__ == "__main__":
    executar_ingestao()
