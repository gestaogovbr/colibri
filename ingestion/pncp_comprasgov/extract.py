"""
Ingestão bruta de dados de compras do ComprasGOV (VW_FT_PNCP_COMPRA) nas três
granularidades: diário, mensal e anual.

Manifesto incremental (manifesto.csv):
  Registra metadados de cada arquivo: view, período, linhas, tamanho, hash SHA-256 e
  timestamp. Nas execuções seguintes, arquivos com hash local igual ao
  manifesto são pulados; se divergirem (corrompido ou fonte atualizada),
  são baixados novamente. A chave do manifesto combina a view com o período no
  formato da granularidade: "2021-12-01" (diário), "2021-12" (mensal), "2021" (anual).

Uso:
  Executado via `colibri pipeline run --apenas pncp-comprasgov`, ou diretamente:
  python -m ingestion.pncp_comprasgov.extract
"""

import csv
import hashlib
import io
import logging
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from utils.baixar_arquivo import baixar_arquivo_do_bucket
from utils.carregar_segredo import carregar_segredo
from utils.salvar_arquivo import salvar_arquivo_no_bucket

import requests

import utils.configurar_logging as log


log.setup_logging()

logger = logging.getLogger(__name__)


# Constantes

DATA_INICIO = date(2021, 12, 1)
DIRETORIO_RAIZ = Path("./dados")
DIRETORIO_SAIDA_DIARIO = Path("./dados/pncp_comprasgov_diario")
DIRETORIO_SAIDA_MENSAL = Path("./dados/pncp_comprasgov_mensal")
DIRETORIO_SAIDA_ANUAL = Path("./dados/pncp_comprasgov_anual")
SEGREDO_NOME = "colibri-token-desenvolvedor"

URL_BASE_DIARIO = "https://repositorio.dados.gov.br/seges/comprasgov/diario"
URL_BASE_MENSAL = "https://repositorio.dados.gov.br/seges/comprasgov/mensal"
URL_BASE_ANUAL = "https://repositorio.dados.gov.br/seges/comprasgov/anual"

# Views extraídas: por ora, só compras (itens e resultados ficam pra depois)
VIEWS = ["VW_FT_PNCP_COMPRA"]
VIEW_PADRAO = VIEWS[0]

TEMPLATE_ARQUIVO_DIARIO = "comprasGOV-diario-{view}-{ano}-{mes:02d}-{dia:02d}.csv"
TEMPLATE_ARQUIVO_MENSAL = "comprasGOV-mensal-{view}-{ano}-{mes:02d}.csv"
TEMPLATE_ARQUIVO_ANUAL = "comprasGOV-anual-{view}-{ano}.csv"

NOME_MANIFESTO = "manifesto.csv"
COLUNAS_MANIFESTO = ["view", "data", "url", "num_linhas", "tamanho_bytes", "num_colunas", "hash_sha256", "extraido_em"]

# Alterações desta execução (arquivos baixados ou atualizados), consumido pelo dbt
# pra inserir só o que mudou nas tabelas staging incrementais.
NOME_ALTERACOES = "pncp_comprasgov_alteracoes.csv"
COLUNAS_ALTERACOES = ["view", "granularidade", "periodo"]

TIMEOUT_SEGUNDOS = 60
MAX_TENTATIVAS = 3
PAUSA_BASE_SEGUNDOS = 5
SALVAR_MANIFESTO_A_CADA = 50


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
        writer = csv.DictWriter(f, fieldnames=COLUNAS_MANIFESTO) # cria objeto pra escrever dicionários com as chaves definidas em COLUNAS_MANIFESTO
        writer.writeheader() # escreve primeira linha do csv (nomes das colunas)
        writer.writerows(sorted(manifesto.values(), key=lambda e: (e["view"], e["data"]))) # escreve as demais linhas, ordenadas


def salvar_alteracoes(caminho: Path, alteracoes: list[tuple[str, str, str]]) -> None:
    """Salva a lista de (view, granularidade, periodo) alterados nesta execução"""
    with open(caminho, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(COLUNAS_ALTERACOES)
        writer.writerows(alteracoes)


def registrar_entrada(manifesto: dict[str, dict], view: str, chave: str, url: str, conteudo: bytes) -> None:
    linhas = list(csv.reader(io.StringIO(conteudo.decode("utf-8"))))
    manifesto[f"{view}:{chave}"] = {
        "view": view,
        "data": chave,
        "url": url,
        "num_linhas": len(linhas) - 1,
        "tamanho_bytes": len(conteudo),
        "num_colunas": len(linhas[0]),
        "hash_sha256": hashlib.sha256(conteudo).hexdigest(),
        "extraido_em": datetime.now().isoformat(timespec="seconds"),
    }


# Hash

def hash_arquivo(caminho: Path) -> str:
    """Calcula SHA-256 do arquivo em blocos de 64 KB para não estourar memória"""
    h = hashlib.sha256()
    with open(caminho, "rb") as f:
        for bloco in iter(lambda: f.read(65_536), b""):
            h.update(bloco)
    return h.hexdigest()


# Download

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

            resposta.raise_for_status() # transforma resposta HTTP de erro em exceção do Python
            return resposta.content

        except requests.exceptions.Timeout:
            logger.warning(f"Timeout (tentativa {tentativa}/{MAX_TENTATIVAS}): {url}")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Erro de rede (tentativa {tentativa}/{MAX_TENTATIVAS}): {e}")

        if tentativa < MAX_TENTATIVAS:
            time.sleep(PAUSA_BASE_SEGUNDOS * tentativa)

    logger.error(f"Falha definitiva: {url}")
    return None


# Processamento

def processar_arquivo(session: requests.Session, view: str, chave: str, url: str, caminho: Path, manifesto: dict[str, dict]) -> str:
    """
    Baixa um arquivo CSV, se necessário.

    Retorna: "baixado", "atualizado", "ignorado" ou "indisponivel"
    """
    entrada = manifesto.get(f"{view}:{chave}")
    conteudo = baixar(session, url)
    if conteudo is None:
        return "indisponivel"
    if entrada and caminho.exists() and entrada["hash_sha256"] == hashlib.sha256(conteudo).hexdigest():
        logger.info(f"[{view}] {chave}: hash bate com manifesto, pulando")
        return "ignorado"
    caminho.parent.mkdir(parents=True, exist_ok=True)
    caminho.write_bytes(conteudo)
    registrar_entrada(manifesto, view, chave, url, conteudo)
    status = "atualizado" if entrada else "baixado"
    e = manifesto[f"{view}:{chave}"]
    logger.info(f"[{view}] {chave}: {status} ({int(e['tamanho_bytes']) / 1_048_576:.2f} MB, {e['num_linhas']} linhas)")
    return status


# Execução

def executar_ingestao() -> None:
    for d in (DIRETORIO_RAIZ, DIRETORIO_SAIDA_DIARIO, DIRETORIO_SAIDA_MENSAL, DIRETORIO_SAIDA_ANUAL):
        d.mkdir(parents=True, exist_ok=True)
    caminho_manifesto = DIRETORIO_RAIZ / NOME_MANIFESTO
    caminho_alteracoes = DIRETORIO_RAIZ / NOME_ALTERACOES
    bucket_nome = carregar_segredo(SEGREDO_NOME)["bucket_lake"]

    manifesto_no_bucket = False
    try:
        baixar_arquivo_do_bucket(NOME_MANIFESTO, bucket_nome, SEGREDO_NOME, str(caminho_manifesto))
        manifesto_no_bucket = True
        logger.info(f"Manifesto baixado do bucket: {bucket_nome}/{NOME_MANIFESTO}")
    except Exception as e:
        logger.warning(f"Manifesto não encontrado no bucket, iniciando do zero: {e}")

    manifesto = carregar_manifesto(caminho_manifesto)
    session = criar_sessao()

    contadores = {"baixado": 0, "atualizado": 0, "ignorado": 0, "indisponivel": 0}
    alteracoes: list[tuple[str, str, str]] = []
    manifesto_modificado = False
    hoje = date.today()
    logger.info(f"Ingestão: {DATA_INICIO} -> {hoje} ({len(VIEWS)} view(s))")

    try:
        i = 1
        for view in VIEWS:

            # Diário
            data = DATA_INICIO
            while data <= hoje:
                status = processar_arquivo(session, view, data.isoformat(), construir_url_diario(view, data), construir_caminho_diario(view, data), manifesto)
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
                status = processar_arquivo(session, view, chave, construir_url_mensal(view, ano, mes), construir_caminho_mensal(view, ano, mes), manifesto)
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
                status = processar_arquivo(session, view, chave, construir_url_anual(view, ano), construir_caminho_anual(view, ano), manifesto)
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
        if manifesto_modificado or not manifesto_no_bucket:
            try:
                salvar_arquivo_no_bucket(str(caminho_manifesto), bucket_nome, SEGREDO_NOME)
            except Exception as e:
                logger.warning(f"Não foi possível salvar manifesto no bucket: {e}")

    logger.info(
        f"Baixados: {contadores['baixado']}  "
        f"Atualizados: {contadores['atualizado']}  "
        f"Ignorados: {contadores['ignorado']}  "
        f"Indisponíveis: {contadores['indisponivel']}"
    )


if __name__ == "__main__":
    executar_ingestao()
