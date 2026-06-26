"""
Ingestão de NFe da CGU (Portal da Transparência).

Para cada período YYYY-MM ainda não processado (ou com conteúdo alterado),
baixa o ZIP correspondente, extrai os CSVs (itens, eventos, nf),
converte latin-1 → UTF-8 e salva em dados/nfe_cgu/{tabela}/{YYYY-MM}.csv.

Manifesto incremental (nfe_cgu_manifesto.csv):
  Registra hash SHA-256 por (tabela, período). Períodos cujo hash bate com o
  manifesto são pulados. O manifesto é sincronizado com o bucket para persistir
  entre ambientes.

Rastreamento de alterações (nfe_cgu_alteracoes.csv):
  Informa ao dbt quais (tabela, período) foram baixados ou atualizados nesta
  execução. Consumido pelos modelos staging como filtro incremental.

Uso:
  python -m ingestion.nfe_cgu.extract
"""

import csv
import hashlib
import io
import logging
import shutil
import time
import tempfile
import zipfile
from datetime import date, datetime
from pathlib import Path

import requests

import utils.configurar_logging as log
from utils.carregar_segredo import carregar_segredo
from utils.manifesto_bucket import baixar_manifesto, subir_manifesto as _subir_manifesto

log.setup_logging()
logger = logging.getLogger(__name__)

URL_BASE = "https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/nfe"
PERIODO_INICIO = date(2022, 1, 1)
TABELAS = ["itens", "eventos", "nf"]
NOME_SEGREDO = "colibri-token-desenvolvedor"

DIRETORIO_DADOS = Path("./dados")
DIRETORIO_NFE = DIRETORIO_DADOS / "nfe_cgu"
DIRETORIO_MANIFESTOS = DIRETORIO_DADOS / "manifestos"
DIRETORIO_ALTERACOES = DIRETORIO_DADOS / "alteracoes"
NOME_MANIFESTO = "nfe_cgu_manifesto.csv"
NOME_ALTERACOES = "nfe_cgu_alteracoes.csv"
COLUNAS_MANIFESTO = ["tabela", "periodo", "hash_sha256", "num_linhas", "tamanho_bytes", "extraido_em"]
COLUNAS_ALTERACOES = ["tabela", "periodo"]

MAX_TENTATIVAS = 3
PAUSA_BASE = 5
TIMEOUT = 120


def _gerar_periodos() -> list[str]:
    hoje = date.today()
    periodos = []
    ano, mes = PERIODO_INICIO.year, PERIODO_INICIO.month
    while (ano, mes) < (hoje.year, hoje.month):
        periodos.append(f"{ano}-{mes:02d}")
        mes += 1
        if mes > 12:
            mes, ano = 1, ano + 1
    return periodos


def _identificar_csvs(nomes: list[str]) -> tuple[str, str, str]:
    itens   = next(n for n in nomes if "item" in n.lower())
    eventos = next(n for n in nomes if "evento" in n.lower())
    nf      = next(n for n in nomes if n not in {itens, eventos})
    return itens, eventos, nf


def _baixar(session: requests.Session, url: str) -> bytes | None:
    for tentativa in range(1, MAX_TENTATIVAS + 1):
        try:
            resp = session.get(url, timeout=TIMEOUT)
            if resp.status_code == 404:
                logger.debug(f"404: {url}")
                return None
            resp.raise_for_status()
            return resp.content
        except requests.RequestException as e:
            logger.warning(f"Tentativa {tentativa}/{MAX_TENTATIVAS}: {e}")
            if tentativa < MAX_TENTATIVAS:
                time.sleep(PAUSA_BASE * tentativa)
    logger.error(f"Falha definitiva: {url}")
    return None


def carregar_manifesto(caminho: Path) -> dict[str, dict]:
    if not caminho.exists():
        return {}
    with open(caminho, newline="", encoding="utf-8") as f:
        return {f"{r['tabela']}:{r['periodo']}": r for r in csv.DictReader(f)}


def salvar_manifesto(caminho: Path, manifesto: dict[str, dict]) -> None:
    with open(caminho, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUNAS_MANIFESTO)
        w.writeheader()
        w.writerows(sorted(manifesto.values(), key=lambda e: (e["tabela"], e["periodo"])))


def salvar_alteracoes(caminho: Path, alteracoes: list[tuple[str, str]]) -> None:
    with open(caminho, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(COLUNAS_ALTERACOES)
        w.writerows(alteracoes)


def resetar_dados_locais() -> None:
    """Apaga os CSVs extraídos, o manifesto e as alterações locais (usado por --do-zero)"""
    shutil.rmtree(DIRETORIO_NFE, ignore_errors=True)
    (DIRETORIO_MANIFESTOS / NOME_MANIFESTO).unlink(missing_ok=True)
    (DIRETORIO_ALTERACOES / NOME_ALTERACOES).unlink(missing_ok=True)


def subir_manifesto() -> None:
    """Sobe o manifesto local pro bucket. Só deve ser chamado depois do dbt rodar com sucesso"""
    bucket = carregar_segredo(NOME_SEGREDO)["bucket_lake"]
    _subir_manifesto(DIRETORIO_MANIFESTOS / NOME_MANIFESTO, NOME_MANIFESTO, bucket, NOME_SEGREDO, logger)


def executar_ingestao() -> bool:
    """Retorna True se algum dado novo foi extraído, False se nada mudou"""
    for tabela in TABELAS:
        (DIRETORIO_NFE / tabela).mkdir(parents=True, exist_ok=True)
    DIRETORIO_MANIFESTOS.mkdir(parents=True, exist_ok=True)
    DIRETORIO_ALTERACOES.mkdir(parents=True, exist_ok=True)

    caminho_manifesto = DIRETORIO_MANIFESTOS / NOME_MANIFESTO
    caminho_alteracoes = DIRETORIO_ALTERACOES / NOME_ALTERACOES
    bucket = carregar_segredo(NOME_SEGREDO)["bucket_lake"]

    baixar_manifesto(caminho_manifesto, NOME_MANIFESTO, bucket, NOME_SEGREDO, logger)
    manifesto = carregar_manifesto(caminho_manifesto)

    periodos = _gerar_periodos()
    logger.info(f"Total: {len(periodos)} períodos")

    contadores = {"baixado": 0, "atualizado": 0, "ignorado": 0, "indisponivel": 0}
    alteracoes: list[tuple[str, str]] = []
    manifesto_modificado = False

    session = requests.Session()
    session.headers["User-Agent"] = "colibri-ingestao/1.0"

    try:
        for periodo in periodos:
            periodo_raw = periodo.replace("-", "")
            url = f"{URL_BASE}/{periodo_raw}_NFe.zip"

            conteudo_zip = _baixar(session, url)
            if conteudo_zip is None:
                contadores["indisponivel"] += 1
                continue

            try:
                with tempfile.TemporaryDirectory() as tmp:
                    tmp_path = Path(tmp)
                    with zipfile.ZipFile(io.BytesIO(conteudo_zip)) as z:
                        z.extractall(tmp)

                    arquivos = [f for f in tmp_path.iterdir() if f.is_file()]
                    if len(arquivos) < 2:
                        logger.error(f"{periodo}: esperado ao menos 2 arquivos, encontrado {len(arquivos)}")
                        continue

                    nomes = [f.name for f in arquivos]
                    try:
                        nome_itens, nome_eventos, nome_nf = _identificar_csvs(nomes)
                    except StopIteration:
                        logger.error(f"{periodo}: não foi possível identificar CSVs: {nomes}")
                        continue

                    salvos = 0
                    for tabela, nome in zip(TABELAS, [nome_itens, nome_eventos, nome_nf]):
                        src = tmp_path / nome
                        conteudo_utf8 = src.read_bytes().decode("latin-1").encode("utf-8")
                        linhas = conteudo_utf8.decode().strip().splitlines()
                        if len(linhas) < 2:
                            logger.debug(f"{periodo}/{tabela}: sem dados, ignorado.")
                            continue

                        hash_atual = hashlib.sha256(conteudo_utf8).hexdigest()
                        chave = f"{tabela}:{periodo}"
                        entrada = manifesto.get(chave)
                        dst = DIRETORIO_NFE / tabela / f"{periodo}.csv"

                        if entrada and dst.exists() and entrada["hash_sha256"] == hash_atual:
                            contadores["ignorado"] += 1
                            continue

                        dst.write_bytes(conteudo_utf8)
                        status = "atualizado" if entrada else "baixado"
                        manifesto[chave] = {
                            "tabela": tabela,
                            "periodo": periodo,
                            "hash_sha256": hash_atual,
                            "num_linhas": len(linhas) - 1,
                            "tamanho_bytes": len(conteudo_utf8),
                            "extraido_em": datetime.now().isoformat(timespec="seconds"),
                        }
                        alteracoes.append((tabela, periodo))
                        contadores[status] += 1
                        manifesto_modificado = True
                        salvos += 1

                if salvos > 0:
                    logger.info(f"{periodo}: {salvos} CSV(s) salvos.")

            except Exception as e:
                logger.error(f"{periodo}: erro ao processar ZIP: {e}")

    finally:
        session.close()
        salvar_manifesto(caminho_manifesto, manifesto)
        salvar_alteracoes(caminho_alteracoes, alteracoes)
        logger.info(
            f"Concluído. {len(set(p for _, p in alteracoes))} períodos novos.  "
            f"Baixados: {contadores['baixado']}  "
            f"Atualizados: {contadores['atualizado']}  "
            f"Ignorados: {contadores['ignorado']}  "
            f"Indisponíveis: {contadores['indisponivel']}"
        )

    return manifesto_modificado


if __name__ == "__main__":
    executar_ingestao()
