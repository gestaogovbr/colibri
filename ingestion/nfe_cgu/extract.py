"""
Ingestão de NFe da CGU (Portal da Transparência).

Para cada período YYYY-MM ainda não processado (ou com conteúdo alterado),
baixa o ZIP correspondente, extrai os CSVs (itens, eventos, nf),
converte latin-1 → UTF-8, converte para Parquet e salva no bucket em nfe_cgu/{tabela}/{YYYY-MM}.parquet.

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

import io
import uuid
import time
import csv
import hashlib
import logging
import shutil
import boto3
import botocore
import duckdb
import tempfile
import zipfile
from datetime import date, datetime
from pathlib import Path
from utils.carregar_segredo import carregar_segredo
from utils.manifesto_bucket import baixar_manifesto, subir_manifesto as _subir_manifesto
from utils.salvar_bytes_no_bucket import salvar_bytes_no_bucket

import requests

import utils.configurar_logging as log

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

TIMEOUT_SEGUNDOS = 120
MAX_TENTATIVAS = 3
PAUSA_BASE_SEGUNDOS = 5
SALVAR_MANIFESTO_A_CADA = 10


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


def csv_para_parquet(conteudo: bytes) -> bytes:
    caminho_tmp = Path(tempfile.gettempdir()) / f"{uuid.uuid4().hex}.parquet"
    try:
        duckdb.read_csv(io.BytesIO(conteudo)).write_parquet(str(caminho_tmp))
        return caminho_tmp.read_bytes()
    finally:
        caminho_tmp.unlink(missing_ok=True)


def criar_sessao() -> requests.Session:
    """Cria uma sessão HTTP persistente pra todas as requests"""
    session = requests.Session()
    session.headers.update({"User-Agent": "colibri-ingestao/1.0"})
    return session


def baixar(session: requests.Session, url: str) -> bytes | None:
    """Baixa a URL com até MAX_TENTATIVAS tentativas e retorna o payload em BYTES"""
    for tentativa in range(1, MAX_TENTATIVAS + 1):
        try:
            resposta = session.get(url, timeout=TIMEOUT_SEGUNDOS)

            if resposta.status_code == 404:
                logger.debug(f"404 — sem dados: {url}")
                return None

            resposta.raise_for_status()
            return resposta.content

        except requests.exceptions.Timeout:
            logger.warning(f"Timeout (tentativa {tentativa}/{MAX_TENTATIVAS}): {url}")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Erro de rede (tentativa {tentativa}/{MAX_TENTATIVAS}): {e}")

        if tentativa < MAX_TENTATIVAS:
            time.sleep(PAUSA_BASE_SEGUNDOS * tentativa)

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


def registrar_entrada(manifesto: dict[str, dict], tabela: str, periodo: str, conteudo: bytes) -> None:
    linhas = conteudo.decode("utf-8").strip().splitlines()
    manifesto[f"{tabela}:{periodo}"] = {
        "tabela": tabela,
        "periodo": periodo,
        "hash_sha256": hashlib.sha256(conteudo).hexdigest(),
        "num_linhas": len(linhas) - 1,
        "tamanho_bytes": len(conteudo),
        "extraido_em": datetime.now().isoformat(timespec="seconds"),
    }


def resetar_dados_locais() -> None:
    """Apaga os CSVs extraídos, o manifesto e as alterações locais (usado por --do-zero)"""
    shutil.rmtree(DIRETORIO_NFE, ignore_errors=True)
    (DIRETORIO_MANIFESTOS / NOME_MANIFESTO).unlink(missing_ok=True)
    (DIRETORIO_ALTERACOES / NOME_ALTERACOES).unlink(missing_ok=True)


def subir_manifesto(bucket: str | None = None) -> None:
    """Sobe o manifesto local pro bucket. Só deve ser chamado depois do dbt rodar com sucesso"""
    bucket = bucket or carregar_segredo(NOME_SEGREDO)["bucket_lake"]
    _subir_manifesto(DIRETORIO_MANIFESTOS / NOME_MANIFESTO, NOME_MANIFESTO, bucket, NOME_SEGREDO, logger)


def processar_periodo(session: requests.Session, periodo: str, manifesto: dict[str, dict], bucket: str) -> str:
    """
    Baixa o ZIP do período, extrai cada tabela em memória e salva no bucket se necessário.

    Retorna: "baixado", "atualizado", "ignorado" ou "indisponivel"
    """
    periodo_raw = periodo.replace("-", "")
    url = f"{URL_BASE}/{periodo_raw}_NFe.zip"

    conteudo_zip = baixar(session, url)
    if conteudo_zip is None:
        return "indisponivel"

    config = carregar_segredo(NOME_SEGREDO)

    # Ex: 'nfe_cgu/itens/2022-01.parquet'
    s3 = boto3.resource(
        "s3",
        endpoint_url=config["endpoint"],
        aws_access_key_id=config["access_key"],
        aws_secret_access_key=config["secret_key"],
        region_name="auto",
    )

    try:
        with zipfile.ZipFile(io.BytesIO(conteudo_zip)) as z:
            nomes = [n for n in z.namelist() if not n.endswith("/")]
            nome_itens, nome_eventos, nome_nf = _identificar_csvs(nomes)

            status_geral = "ignorado"
            for tabela, nome in zip(TABELAS, [nome_itens, nome_eventos, nome_nf]):
                conteudo_utf8 = z.read(nome).decode("latin-1").encode("utf-8")
                linhas = conteudo_utf8.decode().strip().splitlines()
                if len(linhas) < 2:
                    logger.debug(f"[{tabela}] {periodo}: sem dados, ignorado.")
                    continue

                chave = f"{tabela}:{periodo}"
                entrada = manifesto.get(chave)
                nome_no_bucket = f"nfe_cgu/{tabela}/{periodo}.parquet"

                # Verifica se objeto existe no bucket via HEAD request
                try:
                    s3.Object(bucket, nome_no_bucket).load()
                    existe_no_bucket = True
                except botocore.exceptions.ClientError as e:
                    existe_no_bucket = False

                if entrada and existe_no_bucket and entrada["hash_sha256"] == hashlib.sha256(conteudo_utf8).hexdigest():
                    logger.info(f"[{tabela}] {periodo}: hash bate com manifesto, pulando")
                    continue

                conteudo_parquet = csv_para_parquet(conteudo_utf8)
                salvar_bytes_no_bucket(conteudo_parquet, bucket, NOME_SEGREDO, nome_no_bucket)
                registrar_entrada(manifesto, tabela, periodo, conteudo_utf8)
                status_geral = "atualizado" if entrada else "baixado"
                e = manifesto[chave]
                logger.info(f"[{tabela}] {periodo}: {status_geral} ({int(e['tamanho_bytes']) / 1_048_576:.2f} MB, {e['num_linhas']} linhas)")

        return status_geral

    except StopIteration:
        logger.error(f"{periodo}: não foi possível identificar CSVs no ZIP")
        return "indisponivel"
    except Exception as e:
        logger.error(f"{periodo}: erro ao processar ZIP: {e}")
        return "indisponivel"


def executar_ingestao(bucket: str | None = None) -> bool:
    """Retorna True se algum dado novo foi extraído, False se nada mudou"""
    for d in (DIRETORIO_MANIFESTOS, DIRETORIO_ALTERACOES):
        d.mkdir(parents=True, exist_ok=True)
    caminho_manifesto = DIRETORIO_MANIFESTOS / NOME_MANIFESTO
    caminho_alteracoes = DIRETORIO_ALTERACOES / NOME_ALTERACOES
    bucket = bucket or carregar_segredo(NOME_SEGREDO)["bucket_lake"]

    baixar_manifesto(caminho_manifesto, NOME_MANIFESTO, bucket, NOME_SEGREDO, logger)
    manifesto = carregar_manifesto(caminho_manifesto)
    session = criar_sessao()

    contadores = {"baixado": 0, "atualizado": 0, "ignorado": 0, "indisponivel": 0}
    alteracoes: list[tuple[str, str]] = []
    manifesto_modificado = False

    periodos = _gerar_periodos()
    hoje = date.today()
    logger.info(f"Ingestão: {PERIODO_INICIO} -> {hoje} ({len(periodos)} período(s))")

    try:
        for i, periodo in enumerate(periodos, 1):
            status = processar_periodo(session, periodo, manifesto, bucket)
            contadores[status] += 1
            if status in ("baixado", "atualizado"):
                manifesto_modificado = True
                for tabela in TABELAS:
                    alteracoes.append((tabela, periodo))
            if i % SALVAR_MANIFESTO_A_CADA == 0:
                salvar_manifesto(caminho_manifesto, manifesto)

    finally:
        session.close()
        salvar_manifesto(caminho_manifesto, manifesto)
        salvar_alteracoes(caminho_alteracoes, alteracoes)
        logger.info(f"Manifesto: {caminho_manifesto}")
        logger.info(f"Alterações: {caminho_alteracoes} ({len(alteracoes)} arquivo(s))")

    logger.info(
        f"Baixados: {contadores['baixado']}  "
        f"Atualizados: {contadores['atualizado']}  "
        f"Ignorados: {contadores['ignorado']}  "
        f"Indisponíveis: {contadores['indisponivel']}"
    )
    return manifesto_modificado


if __name__ == "__main__":
    executar_ingestao()
