"""
Ingestão dos dados de NCM (Nomenclatura Comum do Mercosul) via API do Siscomex.

Cada execução baixa o JSON completo da API, monta a hierarquia (capítulo/
posição/subposição/item/subitem) em memória e sobe dois arquivos pro bucket,
os dois em raw/ncm/ com timestamp no nome: o JSON bruto e o CSV já
enriquecido. O modelo int_ncm__prefixos do dbt lê só o CSV mais recente.

Uso:
  Executado via `ingestion.ncm.pipeline`, ou diretamente:
  python -m ingestion.ncm.extract
"""

import csv
import json
import logging
from datetime import datetime
from pathlib import Path

import utils.carregar_json_da_url as cju
import utils.configurar_logging as log
from utils.constantes import NOME_SEGREDO_DESENVOLVEDOR
from utils.carregar_segredo import carregar_segredo
from utils.salvar_arquivo_no_bucket import salvar_arquivo_no_bucket
from utils.salvar_bytes_no_bucket import salvar_bytes_no_bucket
from utils.no import No


log.setup_logging()

logger = logging.getLogger(__name__)


# Constantes

NCM_URL = (
    "https://portalunico.siscomex.gov.br/classif/api/publico/nomenclatura/download/json"
)

DIRETORIO_RAIZ = Path("./dados")
DIRETORIO_SAIDA = DIRETORIO_RAIZ / "ncm"
NOME_BASE_ENRIQUECIDO = "ncm_enriquecido"
PREFIXO_RAW = "raw/ncm"


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
    mapa = {
        2: "Capítulo",
        4: "Posição",
        5: "Subposição 1",
        6: "Subposição 2",
        7: "Item",
        8: "Subitem",
    }
    return (num_digitos if num_digitos in mapa else None), mapa.get(
        num_digitos, "Desconhecido"
    )


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
        return next(
            (n for n in caminho[1:] if _obter_nivel(n.codigo)[1] == nivel), None
        )

    def _codigo_sem_ponto(no: No | None) -> str | None:
        return no.codigo.replace(".", "") if no else None

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
        resultado.append(
            {
                "Codigo": no.codigo.replace(".", ""),
                "Descricao": no.descricao,
                "Nivel": nivel,
                "Caminho": no.transformar_caminho_em_string(),
                "Capitulo_Codigo": _codigo_sem_ponto(cap),
                "Capitulo_Descricao": cap.descricao if cap else None,
                "Posicao_Codigo": _codigo_sem_ponto(pos),
                "Posicao_Descricao": pos.descricao if pos else None,
                "Subposicao_1_Codigo": _codigo_sem_ponto(sp1),
                "Subposicao_1_Descricao": sp1.descricao if sp1 else None,
                "Subposicao_2_Codigo": _codigo_sem_ponto(sp2),
                "Subposicao_2_Descricao": sp2.descricao if sp2 else None,
                "Item_Codigo": _codigo_sem_ponto(itm),
                "Item_Descricao": itm.descricao if itm else None,
                "Subitem_Codigo": _codigo_sem_ponto(sub),
                "Subitem_Descricao": sub.descricao if sub else None,
            }
        )
    return resultado


# Bucket


def _subir_raw(dados: dict, agora: datetime, bucket: str) -> None:
    """Sobe o JSON bruto da API (sem transformação) pro bucket, mantendo histórico do dado original"""
    conteudo = json.dumps(dados, ensure_ascii=False).encode("utf-8")
    nome = f"ncm_{agora.strftime('%Y-%m-%d-%H%M%S')}.json"
    salvar_bytes_no_bucket(conteudo, bucket, NOME_SEGREDO_DESENVOLVEDOR, f"{PREFIXO_RAW}/{nome}")
    logger.info(f"Raw subido: {PREFIXO_RAW}/{nome}")


def _subir_enriquecido(caminho_csv: Path, bucket: str) -> None:
    """Sobe o CSV enriquecido pro bucket, junto do JSON bruto, mantendo histórico"""
    nome_no_bucket = f"{PREFIXO_RAW}/{caminho_csv.name}"
    salvar_arquivo_no_bucket(str(caminho_csv), bucket, NOME_SEGREDO_DESENVOLVEDOR, nome_no_bucket)
    logger.info(f"Enriquecido subido: {nome_no_bucket}")


# CSV


def _salvar_enriquecido(enriquecido: list[dict], agora: datetime) -> Path:
    DIRETORIO_SAIDA.mkdir(parents=True, exist_ok=True)
    caminho = (
        DIRETORIO_SAIDA / f"{NOME_BASE_ENRIQUECIDO}_{agora.strftime('%Y-%m-%d-%H%M%S')}.csv"
    )
    with open(caminho, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(enriquecido[0].keys()))
        writer.writeheader()
        writer.writerows(enriquecido)
    logger.info(f"Enriquecido gravado: {caminho} ({len(enriquecido)} linhas)")
    return caminho


# Execução


def executar_ingestao(bucket: str | None = None) -> None:
    """Baixa a NCM da API, enriquece e sobe o raw e o enriquecido pro bucket. Roda sempre, sem dedup."""
    bucket = bucket or carregar_segredo(NOME_SEGREDO_DESENVOLVEDOR)["bucket_lake"]

    dados = cju.carregar_json_da_url(NCM_URL)
    agora = datetime.now()

    mapa_ncm = _flatten_nomenclaturas(dados["Nomenclaturas"])
    arvore = _construir_arvore(mapa_ncm)
    enriquecido = _enriquecer(arvore)
    caminho_csv = _salvar_enriquecido(enriquecido, agora)

    _subir_raw(dados, agora, bucket)
    _subir_enriquecido(caminho_csv, bucket)


if __name__ == "__main__":
    executar_ingestao()
