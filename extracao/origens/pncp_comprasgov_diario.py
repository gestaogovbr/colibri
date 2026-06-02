"""
Ingestão bruta de dados diários do ComprasGOV (VW_FT_PNCP_COMPRA).

Baixa todos os CSVs de compras de dezembro/2021 até hoje do repositório público:
  https://repositorio.dados.gov.br/seges/comprasgov/diario/

Manifesto incremental (manifesto.csv):
  Registra metadados de cada arquivo: data, linhas, tamanho, hash SHA-256 e
  timestamp. Nas execuções seguintes, arquivos com hash local igual ao
  manifesto são pulados; se divergirem (corrompido ou fonte atualizada),
  são baixados novamente.

Uso:
  python pncp_comprasgov_diario.py
"""

import csv
import hashlib
import logging
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

import shared.configurar_logging as log


log.setup_logging()
logger = logging.getLogger(__name__)


# Constantes 

DATA_INICIO = date(2021, 12, 1)
DIRETORIO_SAIDA = Path("./dados/pncp_comprasgov_diario")

URL_BASE = "https://repositorio.dados.gov.br/seges/comprasgov/diario"
TEMPLATE_ARQUIVO = "comprasGOV-diario-VW_FT_PNCP_COMPRA-{ano}-{mes:02d}-{dia:02d}.csv"

NOME_MANIFESTO = "manifesto.csv"
COLUNAS_MANIFESTO = ["data", "url", "num_linhas", "tamanho_bytes", "hash_sha256", "extraido_em"]

TIMEOUT_SEGUNDOS = 60
MAX_TENTATIVAS = 3
PAUSA_BASE_SEGUNDOS = 5     
SALVAR_MANIFESTO_A_CADA = 50


# URLs e caminhos 

def _nome_arquivo(data: date) -> str:
    return TEMPLATE_ARQUIVO.format(ano=data.year, mes=data.month, dia=data.day)


def construir_url(data: date) -> str:
    return f"{URL_BASE}/{data.year}/{data.month:02d}/{data.day:02d}/{_nome_arquivo(data)}"


def construir_caminho(data: date) -> Path:
    """Espelha a estrutura da fonte: diretorio/ano/mm/dd/arquivo.csv"""
    return DIRETORIO_SAIDA / str(data.year) / f"{data.month:02d}" / f"{data.day:02d}" / _nome_arquivo(data)


# Manifesto 

def carregar_manifesto(caminho: Path) -> dict[str, dict]:
    """Lê o manifesto CSV e retorna o dicionário correspondente"""
    if not caminho.exists():
        return {}
    with open(caminho, newline="", encoding="utf-8") as f:
        return {linha["data"]: linha for linha in csv.DictReader(f)}


def salvar_manifesto(caminho: Path, manifesto: dict[str, dict]) -> None:
    """Salva o manifesto em CSV, ordenado por data (do mais antigo ao mais recente)"""
    with open(caminho, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUNAS_MANIFESTO) # cria objeto pra escrever dicionários com as chaves definidas em COLUNAS_MANIFESTO
        writer.writeheader() # escreve primeira linha do csv (nomes das colunas)
        writer.writerows(sorted(manifesto.values(), key=lambda e: e["data"])) # escreve as demais linhas, ordenadas


def registrar_entrada(manifesto: dict[str, dict], data: date, url: str, conteudo: bytes) -> None:
    """ 
    Após baixar e salvar o csv, precisa registrar os metadados no manifesto.
    Pra isso, recebe o conteúdo em bytes e calcula os metadados
    """
    manifesto[data.isoformat()] = {
        "data": data.isoformat(),
        "url": url,
        "num_linhas": conteudo.count(b"\n"),
        "tamanho_bytes": len(conteudo),
        "numero_colunas": conteudo[:conteudo.find(b"\n")].count(b",") + 1, # conta vírgulas na primeira linha (cabeçalho) e soma 1
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


# Processamento por dia 

def processar_dia(session: requests.Session, data: date, manifesto: dict[str, dict]) -> str:
    """
    Baixa o arquivo de compras de uma data, se necessário.

    Retorna um dos seguintes status:
    - "baixado"      : arquivo novo, salvo com sucesso.
    - "atualizado"   : hash divergia do manifesto, arquivo sobrescrito.
    - "ignorado"     : hash local bate com o manifesto, nada a fazer.
    - "indisponivel" : 404 ou falha de rede.
    """
    # String da data fornecida (data -> string) com isoformat
    data_str = data.isoformat()
    
    # Caminho do dia baixado (data -> caminho) com construir_caminho
    caminho = construir_caminho(data)

    # Dicionário formado por uma linha do manifesto, para uma data específica (data_str)
    entrada = manifesto.get(data_str)

    # Monta a URL com base na data
    url = construir_url(data)
    
    # Baixa csv do dia da <<data>>. Conteúdo é uma sequência de bytes (Ex: b'cod_compra,modalidade_id,...')
    conteudo = baixar(session, url)
    
    # Se servidor não responder com carga útil, significa que dia não tem dados. Pular e registrar como "indisponivel"
    if conteudo is None:
        return "indisponivel"
    
    # Se já está no manifesto e o hash bate com o conteúdo baixado, ignorar
    # Se csv não tiver sido baixado ou hash diferir, baixar de novo
    if entrada and caminho.exists() and entrada["hash_sha256"] == hashlib.sha256(conteudo).hexdigest():
        logger.info(f"{data_str}: hash bate com manifesto, pulando")
        return "ignorado"

    # Arquivo novo ou conteúdo alterado na fonte
    caminho.parent.mkdir(parents=True, exist_ok=True) # cria diretório se não existir
    caminho.write_bytes(conteudo) # sobrescreve arquivo local com o que foi baixado
    registrar_entrada(manifesto, data, url, conteudo) # insere um registro no manifesto com os metadados

    status = "atualizado" if entrada else "baixado"
    e = manifesto[data_str]
    logger.info(f"{data_str}: {status} ({int(e['tamanho_bytes']) / 1_048_576:.2f} MB, {e['num_linhas']} linhas)")
    return status


# Execução 

def executar_ingestao() -> None:
    DIRETORIO_SAIDA.mkdir(parents=True, exist_ok=True) 
    caminho_manifesto = DIRETORIO_SAIDA / NOME_MANIFESTO 

    # Cria manifesto (csv) vazio se não existir (1ª execução). Se existir, retorna dict equivalente
    # Ex: { "2021-12-01": {"data": "2021-12-01", "url": "...", "num_linhas": "123", ...}, ... }
    manifesto = carregar_manifesto(caminho_manifesto) 

    # Cria uma única conexão HTTP com o servidor, compartilhada por todas as requests
    session = criar_sessao()

    contadores = {"baixado": 0, "atualizado": 0, "ignorado": 0, "indisponivel": 0}
    logger.info(f"Ingestão: {DATA_INICIO} -> {date.today()}")

    try:
        data = DATA_INICIO
        i = 1
        while data <= date.today():
            # Baixa o CSV do dia, se ainda não tiver sido baixado ou se o hash for diferente
            status = processar_dia(session, data, manifesto)
            contadores[status] += 1 # conta quantos arquivos foram ignorados, baixados ou atualizados

            # Salva o manifesto a cada 'SALVAR_MANIFESTO_A_CADA' dias
            if i % SALVAR_MANIFESTO_A_CADA == 0:
                salvar_manifesto(caminho_manifesto, manifesto)

            data += timedelta(days=1)
            i += 1

    finally:
        # Encerra sessão TCP
        session.close()

        # Salva manifesto atualizado no <<caminho_manifesto>>
        salvar_manifesto(caminho_manifesto, manifesto)
        logger.info(f"Manifesto: {caminho_manifesto}")

    logger.info(
        f"Baixados: {contadores['baixado']}  "
        f"Atualizados: {contadores['atualizado']}  "
        f"Ignorados: {contadores['ignorado']}  "
        f"Indisponíveis: {contadores['indisponivel']}"
    )


if __name__ == "__main__":
    executar_ingestao()
