"""
Ingestão dos dados de NCM (Nomenclatura Comum do Mercosul) via API do Siscomex.

Manifesto incremental (ncm_manifesto.csv):
  Registra metadados de cada extração: hash SHA-256 (excluindo o campo de data),
  basename do CSV gerado, número de registros e timestamp. O hash compara o
  conteúdo atual com a última extração; se idêntico, encerra sem gravar nada.
  O manifesto é sincronizado com o bucket para persistir entre ambientes.
  O arquivo de alterações (ncm_alteracoes.csv) informa ao stg_ncm o basename
  do CSV gerado nesta execução.

Uso:
  Executado via `ingestion.ncm.pipeline`, ou diretamente:
  python -m ingestion.ncm.extract
"""

import csv
import hashlib
import json
import logging
import shutil
from datetime import datetime
from pathlib import Path

import utils.carregar_json_da_url as cju
import utils.configurar_logging as log
from utils.carregar_segredo import carregar_segredo
from utils.manifesto_bucket import baixar_manifesto, subir_manifesto as _subir_manifesto
from utils.no import No


log.setup_logging()

logger = logging.getLogger(__name__)


# Constantes

NCM_URL = "https://portalunico.siscomex.gov.br/classif/api/publico/nomenclatura/download/json"
SEGREDO_NOME = "colibri-token-desenvolvedor"

DIRETORIO_RAIZ = Path("./dados")
DIRETORIO_SAIDA = DIRETORIO_RAIZ / "ncm"
DIRETORIO_MANIFESTOS = DIRETORIO_RAIZ / "manifestos"
DIRETORIO_ALTERACOES = DIRETORIO_RAIZ / "alteracoes"
NOME_BASE_SILVER = "ncm_silver"

NOME_MANIFESTO = "ncm_manifesto.csv"
COLUNAS_MANIFESTO = ["hash_sha256", "arquivo_csv", "num_registros", "extraido_em"]

NOME_ALTERACOES = "ncm_alteracoes.csv"
COLUNAS_ALTERACOES = ["arquivo_csv"]


# Manifesto

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


def salvar_alteracoes(caminho: Path, alteracoes: list[str]) -> None:
    with open(caminho, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(COLUNAS_ALTERACOES)
        for a in alteracoes:
            writer.writerow([a])


# Hash

def _calcular_hash(dados: dict) -> str:
    dados_sem_data = {k: v for k, v in dados.items() if k != "Data_Ultima_Atualizacao_NCM"}
    conteudo = json.dumps(dados_sem_data, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(conteudo.encode()).hexdigest()


# Transformação

def _flatten_nomenclaturas(nomenclaturas: list, resultado: dict | None = None) -> dict:
    if resultado is None:
        resultado = {}
    for item in nomenclaturas:
        resultado[item["Codigo"]] = item["Descricao"]
        filhas = item.get("NomenclaturaFilha") or []
        if filhas:
            _flatten_nomenclaturas(filhas, resultado)
    return resultado


def _obter_nivel(codigo: str) -> tuple[int | None, str]:
    num_digitos = sum(c.isdigit() for c in codigo)
    mapa = {2: "Capítulo", 4: "Posição", 5: "Subposição 1", 6: "Subposição 2", 7: "Item", 8: "Subitem"}
    return (num_digitos if num_digitos in mapa else None), mapa.get(num_digitos, "Desconhecido")


def _obter_pai(codigo: str, mapa_ncm: dict) -> str | None:
    codigo_sem_pontos = codigo.replace(".", "")
    mapa_sem_ponto = {c.replace(".", ""): c for c in mapa_ncm}
    for tamanho in range(len(codigo_sem_pontos) - 1, 0, -1):
        reduzido = codigo_sem_pontos[:tamanho]
        if reduzido in mapa_sem_ponto:
            return mapa_sem_ponto[reduzido]
    return None


def _construir_arvore(mapa_ncm: dict) -> No:
    raiz = No("Raiz NCM", "Nomenclatura Comum do Mercosul")
    niveis = ["Capítulo", "Posição", "Subposição 1", "Subposição 2", "Item", "Subitem"]
    for nivel in niveis:
        for codigo, descricao in mapa_ncm.items():
            if _obter_nivel(codigo)[1] == nivel:
                if nivel == "Capítulo":
                    raiz.adicionar_filho(No(codigo, descricao))
                else:
                    pai_codigo = _obter_pai(codigo, mapa_ncm)
                    if pai_codigo:
                        no_pai = raiz.selecionar_no(pai_codigo)
                        if no_pai is not None:
                            no_pai.adicionar_filho(No(codigo, descricao))
    return raiz


def _enriquecer(arvore: No) -> list[dict]:
    def _primeiro_no_com_nivel(caminho, nivel):
        return next((n for n in caminho[1:] if _obter_nivel(n.codigo)[1] == nivel), None)

    resultado = []
    for no in arvore:
        if no.codigo == "Raiz NCM":
            continue
        _, nivel = _obter_nivel(no.codigo)
        cap = _primeiro_no_com_nivel(no.caminho, "Capítulo")
        pos = _primeiro_no_com_nivel(no.caminho, "Posição")
        sp1 = _primeiro_no_com_nivel(no.caminho, "Subposição 1")
        sp2 = _primeiro_no_com_nivel(no.caminho, "Subposição 2")
        itm = _primeiro_no_com_nivel(no.caminho, "Item")
        sub = _primeiro_no_com_nivel(no.caminho, "Subitem")
        resultado.append({
            "Codigo":                  no.codigo,
            "Descricao":               no.descricao,
            "Nivel":                   nivel,
            "Caminho":                 no.transformar_caminho_em_string(),
            "Capitulo_Codigo":         cap.codigo if cap else None,
            "Capitulo_Descricao":      cap.descricao if cap else None,
            "Posicao_Codigo":          pos.codigo if pos else None,
            "Posicao_Descricao":       pos.descricao if pos else None,
            "Subposicao_1_Codigo":     sp1.codigo if sp1 else None,
            "Subposicao_1_Descricao":  sp1.descricao if sp1 else None,
            "Subposicao_2_Codigo":     sp2.codigo if sp2 else None,
            "Subposicao_2_Descricao":  sp2.descricao if sp2 else None,
            "Item_Codigo":             itm.codigo if itm else None,
            "Item_Descricao":          itm.descricao if itm else None,
            "Subitem_Codigo":          sub.codigo if sub else None,
            "Subitem_Descricao":       sub.descricao if sub else None,
        })
    return resultado


# CSV

def _salvar_silver(enriquecido: list[dict], agora: datetime) -> Path:
    DIRETORIO_SAIDA.mkdir(parents=True, exist_ok=True)
    caminho = DIRETORIO_SAIDA / f"{NOME_BASE_SILVER}_{agora.strftime('%Y-%m-%d-%H%M%S')}.csv"
    with open(caminho, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(enriquecido[0].keys()))
        writer.writeheader()
        writer.writerows(enriquecido)
    logger.info(f"Silver gravado: {caminho} ({len(enriquecido)} linhas)")
    return caminho


# Execução

def resetar_dados_locais() -> None:
    """Apaga os CSVs extraídos, o manifesto e as alterações locais (usado por --do-zero)"""
    shutil.rmtree(DIRETORIO_SAIDA, ignore_errors=True)
    (DIRETORIO_MANIFESTOS / NOME_MANIFESTO).unlink(missing_ok=True)
    (DIRETORIO_ALTERACOES / NOME_ALTERACOES).unlink(missing_ok=True)


def subir_manifesto() -> None:
    """Sobe o manifesto local pro bucket. Só deve ser chamado depois do dbt rodar com sucesso"""
    bucket = carregar_segredo(SEGREDO_NOME)["bucket_lake"]
    _subir_manifesto(DIRETORIO_MANIFESTOS / NOME_MANIFESTO, NOME_MANIFESTO, bucket, SEGREDO_NOME, logger)


def executar_ingestao() -> bool:
    """Retorna True se algum dado novo foi extraído, False se nada mudou"""
    DIRETORIO_SAIDA.mkdir(parents=True, exist_ok=True)
    DIRETORIO_MANIFESTOS.mkdir(parents=True, exist_ok=True)
    DIRETORIO_ALTERACOES.mkdir(parents=True, exist_ok=True)
    caminho_manifesto = DIRETORIO_MANIFESTOS / NOME_MANIFESTO
    caminho_alteracoes = DIRETORIO_ALTERACOES / NOME_ALTERACOES
    bucket = carregar_segredo(SEGREDO_NOME)["bucket_lake"]

    caminho_alteracoes.unlink(missing_ok=True)

    baixar_manifesto(caminho_manifesto, NOME_MANIFESTO, bucket, SEGREDO_NOME, logger)
    manifesto = carregar_manifesto(caminho_manifesto)

    dados = cju.carregar_json_da_url(NCM_URL)
    hash_atual = _calcular_hash(dados)

    ultimo_hash = max(manifesto, key=lambda e: e["extraido_em"])["hash_sha256"] if manifesto else None
    if ultimo_hash == hash_atual:
        logger.info("Sem mudanças nos dados de NCM. Ingestão encerrada.")
        salvar_alteracoes(caminho_alteracoes, [])
        return False

    agora = datetime.now()
    mapa_ncm = _flatten_nomenclaturas(dados["Nomenclaturas"])
    arvore = _construir_arvore(mapa_ncm)
    enriquecido = _enriquecer(arvore)
    caminho_csv = _salvar_silver(enriquecido, agora)

    manifesto.append({
        "hash_sha256": hash_atual,
        "arquivo_csv": caminho_csv.name,
        "num_registros": len(enriquecido),
        "extraido_em": agora.isoformat(timespec="seconds"),
    })
    salvar_manifesto(caminho_manifesto, manifesto)
    salvar_alteracoes(caminho_alteracoes, [caminho_csv.name])

    logger.info(f"Manifesto: {caminho_manifesto} ({len(manifesto)} entrada(s))")
    logger.info(f"Alterações: {caminho_alteracoes} (1 arquivo)")
    return True


if __name__ == "__main__":
    executar_ingestao()
