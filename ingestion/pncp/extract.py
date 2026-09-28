"""
Ingestão bruta dos CABEÇALHOS de contratações direto da API pública do PNCP.

Diferença para o `pncp_comprasgov`: aquele lê o dump do Compras.gov publicado em
repositorio.dados.gov.br, que cobre só o que passa pelo sistema federal. Este lê
o PNCP inteiro — inclusive município que publica por sistema próprio.

Eixo da varredura (`--eixo`):
  atualizacao (padrão)  /contratacoes/atualizacao — janela por data de ATUALIZAÇÃO.
                        É o eixo incremental: pega contratação nova e também
                        alteração de contratação antiga, que é o que o staging
                        incremental + SCD2 do colibri já sabem consumir.
  publicacao            /contratacoes/publicacao — janela por data de PUBLICAÇÃO.
                        Serve para backfill determinístico de um período fechado.

Limites da API medidos em 23/09/2026 (ver docs/nota-pncp-api.md):
  - tamanhoPagina máximo é 50 (100 em diante devolve 400);
  - `codigoModalidadeContratacao` é obrigatório, então cada período é varrido
    uma vez por modalidade;
  - o 429 vem sem `Retry-After` e sem cabeçalho de quota, então o ritmo é
    adaptativo: acelera enquanto passa, recua quando toma 429;
  - de vez em quando a API devolve HTTP 200 com a página CURTA — 20 registros em
    vez de 50, sem aviso nenhum (medido em 27/09/2026 comparando a varredura de
    23/09 com uma nova: as páginas curtas explicam todo o furo). Por isso o
    tamanho de cada página é conferido e página curta é pedida de novo.

Saída (mesmo contrato dos outros extratores do projeto):
  dados/pncp/pncp-{eixo}-{modalidade}-{periodo}.parquet
  dados/manifestos/pncp_manifesto.csv    modalidade, eixo, periodo, num_linhas,
                                         tamanho_bytes, hash_sha256, extraido_em
  dados/alteracoes/pncp_alteracoes.csv   modalidade, eixo, periodo  (filtro do dbt)
  dados/pncp/metricas-{eixo}-{periodo}.json   custo da varredura (requisições, 429, tempo)

Uso:
  python -m ingestion.pncp.extract --ano 2026 --mes 8
  python -m ingestion.pncp.extract --ano 2026 --mes 8 --eixo publicacao --modalidades 8
"""

import argparse
import csv
import hashlib
import json
import logging
import time
from calendar import monthrange
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import requests

import utils.configurar_logging as log

log.setup_logging()

logger = logging.getLogger(__name__)


# Constantes

URL_BASE = "https://pncp.gov.br/api/consulta/v1"

# Só as modalidades com volume relevante; o resto entra quando alguém precisar.
MODALIDADES = {
    4: "Concorrência-Eletrônica",
    6: "Pregão-Eletrônico",
    7: "Pregão-Presencial",
    8: "Dispensa",
    9: "Inexigibilidade",
    12: "Credenciamento",
}

TAMANHO_PAGINA = 50  # máximo aceito pela API
TIMEOUT_SEGUNDOS = 60
MAX_TENTATIVAS = 5
TENTATIVAS_PAGINA_CURTA = 3

# Ritmo adaptativo. Calibrado em 23/09/2026 contra a API: com pausa de 1 s, 16 de 25
# requisições voltam 429; com 2 s, 25 de 25 passam limpas. Por isso o piso é 2 s — a
# adaptação serve para recuar em horário de pico, não para tentar correr mais.
PAUSA_INICIAL = 2.0
PAUSA_MINIMA = 2.0
PAUSA_MAXIMA = 8.0
FATOR_RECUO = 2.0
FATOR_ACELERACAO = 0.9
ACERTOS_PARA_ACELERAR = 20
ESPERA_APOS_429 = 30.0

DIRETORIO_SAIDA = Path("./dados/pncp")
DIRETORIO_MANIFESTOS = Path("./dados/manifestos")
DIRETORIO_ALTERACOES = Path("./dados/alteracoes")

NOME_MANIFESTO = "pncp_manifesto.csv"
COLUNAS_MANIFESTO = ["modalidade", "eixo", "periodo", "num_linhas", "tamanho_bytes", "hash_sha256", "extraido_em"]

NOME_ALTERACOES = "pncp_alteracoes.csv"
COLUNAS_ALTERACOES = ["modalidade", "eixo", "periodo"]

# Campos que vêm como lista e não cabem em coluna escalar; viram JSON texto e o
# dbt desempacota depois, se precisar.
CAMPOS_LISTA = ["fontesOrcamentarias"]


@dataclass
class Custo:
    """Contabilidade da varredura — é o que responde se o backfill cabe no orçamento."""

    requisicoes: int = 0
    erros_429: int = 0
    erros_outros: int = 0
    segundos: float = 0.0
    registros: int = 0
    pausas: list[float] = field(default_factory=list)
    paginas_perdidas: list[tuple[int, int]] = field(default_factory=list)
    paginas_curtas: int = 0
    paginas_curtas_aceitas: list[dict] = field(default_factory=list)
    divergencias: list[dict] = field(default_factory=list)

    def resumo(self) -> dict:
        return {
            "requisicoes": self.requisicoes,
            "erros_429": self.erros_429,
            "erros_outros": self.erros_outros,
            "segundos": round(self.segundos, 1),
            "registros": self.registros,
            "req_por_segundo": round(self.requisicoes / self.segundos, 2) if self.segundos else 0,
            "pausa_final": round(self.pausas[-1], 2) if self.pausas else None,
            "paginas_perdidas": self.paginas_perdidas,
            "paginas_curtas": self.paginas_curtas,
            "paginas_curtas_aceitas": self.paginas_curtas_aceitas,
            "divergencias": self.divergencias,
        }


class Ritmo:
    """Mantém a pausa entre requisições, recuando no 429 e acelerando quando passa limpo."""

    def __init__(self, pausa: float = PAUSA_INICIAL):
        self.pausa = pausa
        self.acertos = 0

    def sucesso(self) -> None:
        self.acertos += 1
        if self.acertos >= ACERTOS_PARA_ACELERAR:
            self.pausa = max(PAUSA_MINIMA, self.pausa * FATOR_ACELERACAO)
            self.acertos = 0

    def recuo(self) -> None:
        self.acertos = 0
        self.pausa = min(PAUSA_MAXIMA, self.pausa * FATOR_RECUO)


# Requisições


def pedir_pagina(
    sessao: requests.Session, eixo: str, params: dict, ritmo: Ritmo, custo: Custo
) -> tuple[dict | None, str]:
    """
    Uma página da API, com recuo no 429.

    Devolve (corpo, situação). A situação distingue os dois jeitos de vir sem corpo:
    "vazia" é a API dizendo que não há nada ali (204), e "falha" é página que não
    conseguimos ler. Confundir as duas foi o que fez a primeira versão perder 150
    registros em silêncio.
    """
    url = f"{URL_BASE}/contratacoes/{eixo}"
    for tentativa in range(1, MAX_TENTATIVAS + 1):
        time.sleep(ritmo.pausa)
        custo.requisicoes += 1
        try:
            resposta = sessao.get(url, params=params, timeout=TIMEOUT_SEGUNDOS)
        except requests.RequestException as erro:
            custo.erros_outros += 1
            logger.warning(f"Rede (tentativa {tentativa}/{MAX_TENTATIVAS}): {erro}")
            time.sleep(ESPERA_APOS_429)
            continue

        if resposta.status_code == 200:
            ritmo.sucesso()
            return resposta.json(), "ok"
        if resposta.status_code == 204:
            ritmo.sucesso()
            return None, "vazia"
        if resposta.status_code == 429:
            custo.erros_429 += 1
            ritmo.recuo()
            espera = ESPERA_APOS_429 * tentativa
            logger.warning(
                f"429 na página {params.get('pagina')} — recuando {espera:.0f}s (pausa agora {ritmo.pausa:.2f}s)"
            )
            time.sleep(espera)
            continue

        custo.erros_outros += 1
        if resposta.status_code >= 500:
            # Blip de gateway numa varredura de dezenas de horas não pode custar uma
            # página inteira; 4xx que não seja 429 é erro nosso e repetir não resolve.
            logger.warning(
                f"HTTP {resposta.status_code} na página {params.get('pagina')} "
                f"(tentativa {tentativa}/{MAX_TENTATIVAS})"
            )
            time.sleep(ESPERA_APOS_429)
            continue
        logger.error(f"HTTP {resposta.status_code} em {params}: {resposta.text[:200]}")
        return None, "falha"

    logger.error(f"Falha definitiva após {MAX_TENTATIVAS} tentativas: {params}")
    return None, "falha"


def esperado_na_pagina(pagina: int, total_registros: int) -> int:
    """Quantos registros uma página tem que trazer: 50, menos na última."""
    return max(0, min(TAMANHO_PAGINA, total_registros - (pagina - 1) * TAMANHO_PAGINA))


def pedir_pagina_inteira(
    sessao: requests.Session,
    eixo: str,
    params: dict,
    total_registros: int,
    ritmo: Ritmo,
    custo: Custo,
    modalidade: int,
    corpo: dict | None = None,
) -> tuple[dict | None, str]:
    """
    Uma página conferida contra o tamanho esperado; curta é pedida de novo.

    Se continuar curta depois das tentativas, fica a maior versão obtida e o caso vai
    para as métricas: pode ser dado vivo de verdade (contratação removida no meio da
    varredura encolhe a última página), e jogar a página fora seria perder mais.
    `corpo` permite conferir uma resposta que já foi pedida (a página 1).
    """
    pagina = params["pagina"]
    esperado = esperado_na_pagina(pagina, total_registros)
    melhor = None
    for tentativa in range(TENTATIVAS_PAGINA_CURTA + 1):
        if corpo is None or tentativa > 0:
            corpo, situacao = pedir_pagina(sessao, eixo, params, ritmo, custo)
            if not corpo:
                if melhor is None:
                    return None, situacao
                break
        if len(corpo["data"]) >= esperado:
            return corpo, "ok"
        custo.paginas_curtas += 1
        logger.warning(
            f"Página {pagina} veio curta: {len(corpo['data'])} de {esperado} "
            f"(tentativa {tentativa + 1}/{TENTATIVAS_PAGINA_CURTA + 1})"
        )
        if melhor is None or len(corpo["data"]) > len(melhor["data"]):
            melhor = corpo
    custo.paginas_curtas_aceitas.append(
        {"modalidade": modalidade, "pagina": pagina, "obtido": len(melhor["data"]), "esperado": esperado}
    )
    return melhor, "ok"


def varrer(
    sessao: requests.Session,
    eixo: str,
    modalidade: int,
    data_inicial: date,
    data_final: date,
    ritmo: Ritmo,
    custo: Custo,
) -> list[dict]:
    """Varre todas as páginas de uma modalidade num período e devolve os registros crus."""
    params = {
        "dataInicial": data_inicial.strftime("%Y%m%d"),
        "dataFinal": data_final.strftime("%Y%m%d"),
        "codigoModalidadeContratacao": modalidade,
        "pagina": 1,
        "tamanhoPagina": TAMANHO_PAGINA,
    }
    primeira, _ = pedir_pagina(sessao, eixo, params, ritmo, custo)
    if not primeira or primeira.get("empty"):
        logger.info(f"{MODALIDADES.get(modalidade, modalidade)}: nada no período")
        return []

    total_paginas = primeira["totalPaginas"]
    total_registros = primeira["totalRegistros"]
    primeira, _ = pedir_pagina_inteira(sessao, eixo, params, total_registros, ritmo, custo, modalidade, primeira)
    registros = list(primeira["data"])
    logger.info(
        f"{MODALIDADES.get(modalidade, modalidade)}: {primeira['totalRegistros']:,} registros "
        f"em {total_paginas:,} páginas".replace(",", ".")
    )

    pendentes: list[int] = []
    for pagina in range(2, total_paginas + 1):
        params["pagina"] = pagina
        corpo, situacao = pedir_pagina_inteira(sessao, eixo, params, total_registros, ritmo, custo, modalidade)
        if corpo:
            registros.extend(corpo["data"])
        elif situacao == "falha":
            pendentes.append(pagina)
        if pagina % 100 == 0:
            andamento = f"  página {pagina:,}/{total_paginas:,}".replace(",", ".")
            logger.info(f"{andamento} (pausa {ritmo.pausa:.2f}s, 429 até aqui: {custo.erros_429})")

    # Segunda passada nas páginas que falharam, em ritmo penitente. Sem isso, um 429
    # teimoso vira buraco no dado sem ninguém ficar sabendo.
    if pendentes:
        logger.warning(f"Repescagem de {len(pendentes)} página(s): {pendentes[:20]}")
        ritmo.pausa = min(PAUSA_MAXIMA, max(ritmo.pausa, PAUSA_INICIAL) * FATOR_RECUO)
        ainda_faltando = []
        for pagina in pendentes:
            params["pagina"] = pagina
            corpo, _ = pedir_pagina_inteira(sessao, eixo, params, total_registros, ritmo, custo, modalidade)
            if corpo:
                registros.extend(corpo["data"])
            else:
                ainda_faltando.append(pagina)
        if ainda_faltando:
            custo.paginas_perdidas.extend([(modalidade, p) for p in ainda_faltando])
            logger.error(f"Páginas perdidas na modalidade {modalidade}: {ainda_faltando}")

    esperado = total_registros
    if len(registros) != esperado:
        # Com as páginas curtas repedidas, o que sobra é dado vivo: contratação incluída
        # ou removida no meio da varredura desloca as páginas. Diferença pequena é isso;
        # diferença grande é erro nosso.
        custo.divergencias.append({"modalidade": modalidade, "esperado": esperado, "obtido": len(registros)})
        nivel = logger.error if abs(len(registros) - esperado) > TAMANHO_PAGINA else logger.warning
        nivel(f"Modalidade {modalidade}: esperados {esperado} registros, obtidos {len(registros)}")

    custo.registros += len(registros)
    return registros


# Gravação


def normalizar(registros: list[dict]) -> pd.DataFrame:
    """Achata os objetos aninhados (orgaoEntidade, unidadeOrgao, amparoLegal) em colunas."""
    quadro = pd.json_normalize(registros, sep="_")

    def como_json(valor):
        return json.dumps(valor, ensure_ascii=False) if isinstance(valor, list) else None

    for coluna in CAMPOS_LISTA:
        if coluna in quadro.columns:
            quadro[coluna] = quadro[coluna].apply(como_json)
    return quadro


def salvar_parquet(quadro: pd.DataFrame, caminho: Path) -> tuple[int, str]:
    """Grava o parquet e devolve (tamanho em bytes, hash sha-256)."""
    caminho.parent.mkdir(parents=True, exist_ok=True)
    quadro.to_parquet(caminho, index=False)
    conteudo = caminho.read_bytes()
    return len(conteudo), hashlib.sha256(conteudo).hexdigest()


def escrever_csv(caminho: Path, colunas: list[str], linhas: list[dict]) -> None:
    caminho.parent.mkdir(parents=True, exist_ok=True)
    novo = not caminho.exists()
    with caminho.open("a", newline="", encoding="utf-8") as arquivo:
        escritor = csv.DictWriter(arquivo, fieldnames=colunas)
        if novo:
            escritor.writeheader()
        escritor.writerows(linhas)


# Orquestração


def executar(ano: int, mes: int, eixo: str, modalidades: list[int]) -> dict:
    data_inicial = date(ano, mes, 1)
    data_final = date(ano, mes, monthrange(ano, mes)[1])
    periodo = f"{ano}-{mes:02d}"

    sessao = requests.Session()
    sessao.headers.update({"Accept": "application/json"})
    ritmo = Ritmo()
    custo = Custo()
    inicio = time.monotonic()

    manifesto: list[dict] = []
    alteracoes: list[dict] = []
    for modalidade in modalidades:
        registros = varrer(sessao, eixo, modalidade, data_inicial, data_final, ritmo, custo)
        custo.pausas.append(ritmo.pausa)
        if not registros:
            continue
        caminho = DIRETORIO_SAIDA / f"pncp-{eixo}-{modalidade}-{periodo}.parquet"
        tamanho, digest = salvar_parquet(normalizar(registros), caminho)
        manifesto.append(
            {
                "modalidade": modalidade,
                "eixo": eixo,
                "periodo": periodo,
                "num_linhas": len(registros),
                "tamanho_bytes": tamanho,
                "hash_sha256": digest,
                "extraido_em": datetime.now().isoformat(timespec="seconds"),
            }
        )
        alteracoes.append({"modalidade": modalidade, "eixo": eixo, "periodo": periodo})

    custo.segundos = time.monotonic() - inicio
    if manifesto:
        escrever_csv(DIRETORIO_MANIFESTOS / NOME_MANIFESTO, COLUNAS_MANIFESTO, manifesto)
        escrever_csv(DIRETORIO_ALTERACOES / NOME_ALTERACOES, COLUNAS_ALTERACOES, alteracoes)

    metricas = {
        "eixo": eixo,
        "periodo": periodo,
        "modalidades": modalidades,
        "custo": custo.resumo(),
        "arquivos": manifesto,
    }
    DIRETORIO_SAIDA.mkdir(parents=True, exist_ok=True)
    (DIRETORIO_SAIDA / f"metricas-{eixo}-{periodo}.json").write_text(
        json.dumps(metricas, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    logger.info(f"Custo da varredura: {custo.resumo()}")
    return metricas


def main() -> None:
    analisador = argparse.ArgumentParser(description="Extrai cabeçalhos de contratações da API pública do PNCP")
    analisador.add_argument("--ano", type=int, required=True)
    analisador.add_argument("--mes", type=int, required=True)
    analisador.add_argument("--eixo", choices=["atualizacao", "publicacao"], default="atualizacao")
    analisador.add_argument(
        "--modalidades",
        type=lambda v: [int(x) for x in v.split(",")],
        default=list(MODALIDADES),
        help="Códigos separados por vírgula (padrão: todas as relevantes)",
    )
    argumentos = analisador.parse_args()
    executar(argumentos.ano, argumentos.mes, argumentos.eixo, argumentos.modalidades)


if __name__ == "__main__":
    main()
