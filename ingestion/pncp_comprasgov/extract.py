"""
Ingestão bruta de dados de compras do ComprasGOV (VW_FT_PNCP_COMPRA) nas três
granularidades: diário, mensal e anual.

Manifesto incremental (pncp_comprasgov_manifesto.csv):
  Registra metadados de cada arquivo: view, período, linhas, tamanho, hash SHA-256 e
  timestamp. Nas execuções seguintes, arquivos com hash local igual ao
  manifesto são pulados; se divergirem (corrompido ou fonte atualizada),
  são baixados novamente. A chave do manifesto combina a view com o período no
  formato da granularidade: "2021-12-01" (diário), "2021-12" (mensal), "2021" (anual).

Checagem barata (HEAD/ETag):
  A verificação cobre o histórico inteiro a cada execução, mas o download só
  acontece quando a fonte mudou de fato: para cada arquivo cujo ETag está no
  manifesto, um HEAD confere se o ETag da fonte é o mesmo e se o parquet segue
  no bucket; em caso positivo o arquivo é pulado sem baixar. Qualquer dúvida
  (sem ETag, HEAD falhou, objeto sumiu do bucket, ETag diferente) cai no GET
  de sempre, com conferência de hash. COLIBRI_VERIFICACAO_COMPLETA=1 desliga o
  atalho e força o GET em tudo.

Uso:
  Executado via `colibri pipeline run --apenas pncp-comprasgov`, ou diretamente:
  python -m ingestion.pncp_comprasgov.extract
"""

import csv
import hashlib
import io
import logging
import os
import shutil
import tempfile
import time
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

import botocore
import duckdb
import requests

import utils.configurar_logging as log
from utils.carregar_segredo import carregar_segredo
from utils.constantes import NOME_SEGREDO_DESENVOLVEDOR
from utils.criar_cliente import criar_cliente
from utils.manifesto_bucket import baixar_manifesto
from utils.manifesto_bucket import subir_manifesto as _subir_manifesto
from utils.salvar_bytes_no_bucket import salvar_bytes_no_bucket

log.setup_logging()

logger = logging.getLogger(__name__)


# Constantes

DATA_INICIO = date(2021, 12, 1)
DIRETORIO_RAIZ = Path("./dados")
DIRETORIO_SAIDA_DIARIO = Path("./dados/pncp_comprasgov_diario")
DIRETORIO_SAIDA_MENSAL = Path("./dados/pncp_comprasgov_mensal")
DIRETORIO_SAIDA_ANUAL = Path("./dados/pncp_comprasgov_anual")
DIRETORIO_MANIFESTOS = DIRETORIO_RAIZ / "manifestos"
DIRETORIO_ALTERACOES_DIR = DIRETORIO_RAIZ / "alteracoes"

URL_BASE_DIARIO = "https://repositorio.dados.gov.br/seges/comprasgov/diario"
URL_BASE_MENSAL = "https://repositorio.dados.gov.br/seges/comprasgov/mensal"
URL_BASE_ANUAL = "https://repositorio.dados.gov.br/seges/comprasgov/anual"

VIEWS = ["VW_FT_PNCP_COMPRA", "VW_FT_PNCP_COMPRA_ITEM", "VW_DM_PNCP_ITEM_RESULTADO"]
VIEW_PADRAO = VIEWS[0]

TEMPLATE_ARQUIVO_DIARIO = "comprasGOV-diario-{view}-{ano}-{mes:02d}-{dia:02d}.csv"
TEMPLATE_ARQUIVO_MENSAL = "comprasGOV-mensal-{view}-{ano}-{mes:02d}.csv"
TEMPLATE_ARQUIVO_ANUAL = "comprasGOV-anual-{view}-{ano}.csv"

NOME_MANIFESTO = "pncp_comprasgov_manifesto.csv"
COLUNAS_MANIFESTO = [
    "view",
    "data",
    "url",
    "num_linhas",
    "tamanho_bytes",
    "num_colunas",
    "hash_sha256",
    "extraido_em",
    "etag",
    "last_modified",
]

# Alterações desta execução (arquivos baixados ou atualizados), consumido pelo dbt
# pra inserir só o que mudou nas tabelas staging incrementais.
NOME_ALTERACOES = "pncp_comprasgov_alteracoes.csv"
COLUNAS_ALTERACOES = ["view", "granularidade", "periodo"]

TIMEOUT_SEGUNDOS = 60
MAX_TENTATIVAS = 3
PAUSA_BASE_SEGUNDOS = 5
SALVAR_MANIFESTO_A_CADA = 50

# Modo paranoico: ignora o atalho HEAD/ETag e baixa tudo pra conferir o hash,
# como antes. Útil diante de qualquer suspeita sobre os ETags da fonte.
VERIFICACAO_COMPLETA = os.environ.get("COLIBRI_VERIFICACAO_COMPLETA", "").strip().lower() in ("1", "true", "sim")


# URLs e caminhos


def construir_url_diario(view: str, data: date) -> str:
    nome = TEMPLATE_ARQUIVO_DIARIO.format(view=view, ano=data.year, mes=data.month, dia=data.day)
    return f"{URL_BASE_DIARIO}/{data.year}/{data.month:02d}/{data.day:02d}/{nome}"


def construir_caminho_diario(view: str, data: date) -> Path:
    nome = TEMPLATE_ARQUIVO_DIARIO.format(view=view, ano=data.year, mes=data.month, dia=data.day)
    return DIRETORIO_SAIDA_DIARIO / str(data.year) / f"{data.month:02d}" / f"{data.day:02d}" / nome


def construir_url_mensal(view: str, ano: int, mes: int) -> str:
    nome = TEMPLATE_ARQUIVO_MENSAL.format(view=view, ano=ano, mes=mes)
    return f"{URL_BASE_MENSAL}/{ano}/{mes:02d}/{nome}"


def construir_caminho_mensal(view: str, ano: int, mes: int) -> Path:
    nome = TEMPLATE_ARQUIVO_MENSAL.format(view=view, ano=ano, mes=mes)
    return DIRETORIO_SAIDA_MENSAL / str(ano) / f"{mes:02d}" / nome


def construir_url_anual(view: str, ano: int) -> str:
    nome = TEMPLATE_ARQUIVO_ANUAL.format(view=view, ano=ano)
    return f"{URL_BASE_ANUAL}/{ano}/{nome}"


def construir_caminho_anual(view: str, ano: int) -> Path:
    nome = TEMPLATE_ARQUIVO_ANUAL.format(view=view, ano=ano)
    return DIRETORIO_SAIDA_ANUAL / str(ano) / nome


# Manifesto


def carregar_manifesto(caminho: Path) -> dict[str, dict]:
    """Lê o manifesto CSV e retorna o dicionário correspondente, indexado por "view:data" """
    if not caminho.exists():
        return {}
    manifesto: dict[str, dict] = {}
    with open(caminho, newline="", encoding="utf-8") as f:
        for linha in csv.DictReader(f):
            # Manifestos antigos (antes de itens/resultados) não tinham a coluna "view"
            view = linha.get("view") or VIEW_PADRAO
            linha["view"] = view
            manifesto[f"{view}:{linha['data']}"] = linha
    return manifesto


def salvar_manifesto(caminho: Path, manifesto: dict[str, dict]) -> None:
    """Salva o manifesto em CSV, ordenado por view e data (do mais antigo ao mais recente)"""
    with open(caminho, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=COLUNAS_MANIFESTO, restval=""
        )  # cria objeto pra escrever dicionários com as chaves definidas em COLUNAS_MANIFESTO
        # restval="" preenche colunas novas (etag/last_modified) em manifestos antigos
        writer.writeheader()  # escreve primeira linha do csv (nomes das colunas)
        writer.writerows(
            sorted(manifesto.values(), key=lambda e: (e["view"], e["data"]))
        )  # escreve as demais linhas, ordenadas


def salvar_alteracoes(caminho: Path, alteracoes: list[tuple[str, str, str]]) -> None:
    """Salva a lista de (view, granularidade, periodo) alterados nesta execução"""
    with open(caminho, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(COLUNAS_ALTERACOES)
        writer.writerows(alteracoes)


def registrar_entrada(
    manifesto: dict[str, dict],
    view: str,
    chave: str,
    url: str,
    conteudo: bytes,
    etag: str = "",
    last_modified: str = "",
) -> None:
    reader = csv.reader(io.StringIO(conteudo.decode("utf-8")))
    header = next(reader, [])
    num_linhas = sum(1 for _ in reader)
    manifesto[f"{view}:{chave}"] = {
        "view": view,
        "data": chave,
        "url": url,
        "num_linhas": num_linhas,
        "tamanho_bytes": len(conteudo),
        "num_colunas": len(header),
        "hash_sha256": hashlib.sha256(conteudo).hexdigest(),
        "extraido_em": datetime.now().isoformat(timespec="seconds"),
        "etag": etag,
        "last_modified": last_modified,
    }


# Hash


def hash_arquivo(caminho: Path) -> str:
    """Calcula SHA-256 do arquivo em blocos de 64 KB para não estourar memória"""
    h = hashlib.sha256()
    with open(caminho, "rb") as f:
        for bloco in iter(lambda: f.read(65_536), b""):
            h.update(bloco)
    return h.hexdigest()


# Conversão


def csv_para_parquet(conteudo: bytes) -> bytes:
    # Cria o caminho para um parquet num diretório temporário do SO
    caminho_tmp = Path(tempfile.gettempdir()) / f"{uuid.uuid4().hex}.parquet"
    try:
        # Lê o csv e escreve ele como um parquet no arquivo temporário
        duckdb.read_csv(io.BytesIO(conteudo), sample_size=-1).write_parquet(str(caminho_tmp))
        # Lê os bytes do parquet
        return caminho_tmp.read_bytes()
    finally:
        caminho_tmp.unlink(missing_ok=True)


# Download


def criar_sessao() -> requests.Session:
    """Cria uma sessão HTTP persistente pra todas as requests"""
    session = requests.Session()
    session.headers.update({"User-Agent": "colibri-ingestao/1.0"})
    return session


def baixar(session: requests.Session, url: str) -> tuple[bytes, dict] | tuple[None, None]:
    """Baixa a URL com até MAX_TENTATIVAS tentativas e retorna (payload em BYTES, headers)"""
    for tentativa in range(1, MAX_TENTATIVAS + 1):
        try:
            resposta = session.get(url, timeout=TIMEOUT_SEGUNDOS)

            if resposta.status_code == 404:
                logger.debug(f"404 — sem dados: {url}")
                return None, None

            resposta.raise_for_status()  # transforma resposta HTTP de erro em exceção do Python
            return resposta.content, resposta.headers

        except requests.exceptions.Timeout:
            logger.warning(f"Timeout (tentativa {tentativa}/{MAX_TENTATIVAS}): {url}")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Erro de rede (tentativa {tentativa}/{MAX_TENTATIVAS}): {e}")

        if tentativa < MAX_TENTATIVAS:
            time.sleep(PAUSA_BASE_SEGUNDOS * tentativa)

    logger.error(f"Falha definitiva: {url}")
    return None, None


class FalhaHead(Exception):
    """O HEAD não obteve resposta útil (rede, timeout, erro HTTP). Quem chama
    deve cair pro GET de sempre: a falha do atalho nunca pode impedir a
    verificação completa."""


def obter_cabecalhos(session: requests.Session, url: str) -> dict | None:
    """Faz um HEAD na URL pra checar se o arquivo mudou sem baixá-lo.

    Retorna os headers, ou None se a fonte respondeu 404. Levanta FalhaHead
    em qualquer outro problema (sem tentativas: o GET já tem as suas).
    """
    try:
        resposta = session.head(url, timeout=TIMEOUT_SEGUNDOS)
        if resposta.status_code == 404:
            logger.debug(f"404 — sem dados: {url}")
            return None
        resposta.raise_for_status()
        return resposta.headers
    except requests.exceptions.RequestException as e:
        raise FalhaHead(str(e)) from e


# Processamento


def existe_no_bucket(cliente, bucket_nome: str, nome_no_bucket: str) -> bool:
    """Verifica se o objeto existe no bucket via HEAD request (HeadObject).

    `cliente` é o cliente S3 criado uma vez por execução em executar_ingestao —
    são centenas de checagens por varredura, não vale recriar cliente e reler
    o segredo a cada uma.
    """
    try:
        cliente.head_object(Bucket=bucket_nome, Key=nome_no_bucket)
        return True
    except botocore.exceptions.ClientError:
        return False


def processar_arquivo(
    session: requests.Session,
    view: str,
    chave: str,
    url: str,
    caminho: Path,
    manifesto: dict[str, dict],
    bucket_nome: str,
    cliente,
) -> str:
    """
    Baixa um arquivo CSV e salva no bucket, se necessário.

    Se o manifesto já registra o ETag do arquivo, checa antes com um HEAD se a
    fonte mudou — a verificação completa continua cobrindo todo o histórico,
    mas o GET (pesado) só acontece quando há mudança de fato. Qualquer dúvida
    (sem ETag, HEAD falhou, objeto sumiu do bucket) cai pro GET de sempre.
    Um HEAD 404 numa entrada conhecida significa que o arquivo sumiu da fonte:
    conta como "indisponivel", o mesmo veredito que o GET 404 sempre deu.

    Retorna: "baixado", "atualizado", "ignorado" ou "indisponivel"
    """
    entrada = manifesto.get(f"{view}:{chave}")

    # Ex: 'dados/pncp_comprasgov_diario/2021/12/01/comprasGOV-diario-VW_FT_PNCP_COMPRA-2021-12-01.csv'
    nome_no_bucket = caminho.relative_to(DIRETORIO_RAIZ).with_suffix(".parquet").as_posix()

    etag_conhecido = entrada.get("etag") if entrada else ""
    if etag_conhecido and not VERIFICACAO_COMPLETA:
        try:
            cabecalhos = obter_cabecalhos(session, url)
        except FalhaHead as e:
            logger.warning(f"[{view}] {chave}: HEAD falhou, caindo pro GET ({e})")
        else:
            if cabecalhos is None:
                logger.debug(f"[{view}] {chave}: HEAD 404, arquivo sumiu da fonte")
                return "indisponivel"
            if cabecalhos.get("ETag") == etag_conhecido and existe_no_bucket(cliente, bucket_nome, nome_no_bucket):
                logger.info(f"[{view}] {chave}: ETag bate com manifesto, pulando sem baixar")
                return "ignorado"

    conteudo, cabecalhos = baixar(session, url)
    if conteudo is None:
        return "indisponivel"
    etag = cabecalhos.get("ETag", "")
    last_modified = cabecalhos.get("Last-Modified", "")

    # Salva no bucket caso arquivo conste no manifesto, no bucket E hash bater com manifesto
    if (
        entrada
        and existe_no_bucket(cliente, bucket_nome, nome_no_bucket)
        and entrada["hash_sha256"] == hashlib.sha256(conteudo).hexdigest()
    ):
        # Conteúdo igual: só aprende/renova o ETag pra próxima rodada pular o GET
        entrada["etag"] = etag
        entrada["last_modified"] = last_modified
        logger.info(f"[{view}] {chave}: hash bate com manifesto, pulando")
        return "ignorado"
    else:
        conteudo_parquet = csv_para_parquet(conteudo)
        salvar_bytes_no_bucket(conteudo_parquet, bucket_nome, NOME_SEGREDO_DESENVOLVEDOR, nome_no_bucket)
        registrar_entrada(manifesto, view, chave, url, conteudo, etag, last_modified)
        status = "atualizado" if entrada else "baixado"
        e = manifesto[f"{view}:{chave}"]
        logger.info(
            f"[{view}] {chave}: {status} ({int(e['tamanho_bytes']) / 1_048_576:.2f} MB, {e['num_linhas']} linhas)"
        )
        return status


# Execução


def resetar_dados_locais() -> None:
    """Apaga os CSVs extraídos, o manifesto e as alterações locais (usado por --do-zero)"""
    for diretorio in (
        DIRETORIO_SAIDA_DIARIO,
        DIRETORIO_SAIDA_MENSAL,
        DIRETORIO_SAIDA_ANUAL,
    ):
        shutil.rmtree(diretorio, ignore_errors=True)
    (DIRETORIO_MANIFESTOS / NOME_MANIFESTO).unlink(missing_ok=True)
    (DIRETORIO_ALTERACOES_DIR / NOME_ALTERACOES).unlink(missing_ok=True)


def subir_manifesto(bucket_nome: str | None = None) -> None:
    """Sobe o manifesto local pro bucket. Só deve ser chamado depois do dbt rodar com sucesso"""
    bucket_nome = bucket_nome or carregar_segredo(NOME_SEGREDO_DESENVOLVEDOR)["bucket_lake"]
    _subir_manifesto(
        DIRETORIO_MANIFESTOS / NOME_MANIFESTO,
        NOME_MANIFESTO,
        bucket_nome,
        NOME_SEGREDO_DESENVOLVEDOR,
        logger,
    )


def executar_ingestao(bucket_nome: str | None = None) -> bool:
    """Retorna True se algum dado novo foi extraído, False se nada mudou"""
    for d in (
        DIRETORIO_RAIZ,
        DIRETORIO_SAIDA_DIARIO,
        DIRETORIO_SAIDA_MENSAL,
        DIRETORIO_SAIDA_ANUAL,
        DIRETORIO_MANIFESTOS,
        DIRETORIO_ALTERACOES_DIR,
    ):
        d.mkdir(parents=True, exist_ok=True)
    caminho_manifesto = DIRETORIO_MANIFESTOS / NOME_MANIFESTO
    caminho_alteracoes = DIRETORIO_ALTERACOES_DIR / NOME_ALTERACOES
    bucket_nome = bucket_nome or carregar_segredo(NOME_SEGREDO_DESENVOLVEDOR)["bucket_lake"]

    baixar_manifesto(
        caminho_manifesto,
        NOME_MANIFESTO,
        bucket_nome,
        NOME_SEGREDO_DESENVOLVEDOR,
        logger,
    )
    manifesto = carregar_manifesto(caminho_manifesto)
    session = criar_sessao()
    cliente = criar_cliente(carregar_segredo(NOME_SEGREDO_DESENVOLVEDOR))

    contadores = {"baixado": 0, "atualizado": 0, "ignorado": 0, "indisponivel": 0}
    alteracoes: list[tuple[str, str, str]] = []
    manifesto_modificado = False
    hoje = date.today()
    logger.info(f"Ingestão: {DATA_INICIO} -> {hoje} ({len(VIEWS)} view(s))")
    if VERIFICACAO_COMPLETA:
        logger.info("COLIBRI_VERIFICACAO_COMPLETA ativo: ignorando ETags, baixando tudo pra conferir hash")

    try:
        i = 1
        for view in VIEWS:
            # Diário
            data = DATA_INICIO
            while data <= hoje:
                status = processar_arquivo(
                    session,
                    view,
                    data.isoformat(),
                    construir_url_diario(view, data),
                    construir_caminho_diario(view, data),
                    manifesto,
                    bucket_nome,
                    cliente,
                )
                contadores[status] += 1
                if status in ("baixado", "atualizado"):
                    manifesto_modificado = True
                    alteracoes.append((view, "diario", data.isoformat()))
                if i % SALVAR_MANIFESTO_A_CADA == 0:
                    salvar_manifesto(caminho_manifesto, manifesto)
                data += timedelta(days=1)
                i += 1

            # Mensal
            ano, mes = DATA_INICIO.year, DATA_INICIO.month
            while (ano, mes) <= (hoje.year, hoje.month):
                chave = f"{ano}-{mes:02d}"
                status = processar_arquivo(
                    session,
                    view,
                    chave,
                    construir_url_mensal(view, ano, mes),
                    construir_caminho_mensal(view, ano, mes),
                    manifesto,
                    bucket_nome,
                    cliente,
                )
                contadores[status] += 1
                if status in ("baixado", "atualizado"):
                    manifesto_modificado = True
                    alteracoes.append((view, "mensal", chave))
                if i % SALVAR_MANIFESTO_A_CADA == 0:
                    salvar_manifesto(caminho_manifesto, manifesto)
                mes += 1
                if mes > 12:
                    mes, ano = 1, ano + 1
                i += 1

            # Anual
            for ano in range(DATA_INICIO.year, hoje.year + 1):
                chave = str(ano)
                status = processar_arquivo(
                    session,
                    view,
                    chave,
                    construir_url_anual(view, ano),
                    construir_caminho_anual(view, ano),
                    manifesto,
                    bucket_nome,
                    cliente,
                )
                contadores[status] += 1
                if status in ("baixado", "atualizado"):
                    manifesto_modificado = True
                    alteracoes.append((view, "anual", chave))
                if i % SALVAR_MANIFESTO_A_CADA == 0:
                    salvar_manifesto(caminho_manifesto, manifesto)
                i += 1

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
