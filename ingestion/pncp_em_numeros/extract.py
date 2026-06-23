"""
Extração das tabelas/views "PNCP em números" via Databricks SQL.

Lê 4 tabelas brutas (catalog raw, schema geral) e 3 views agregadas (catalog
cotin_dlt_pncp, schema bronze) e salva cada uma particionada em lotes parquet
em dados/pncp_em_numeros/.

Particionamento em lotes (necessário pro dbt incremental com append):
  Cada fonte é dividida em lotes por FAIXA DE ID (lote = chave // TAMANHO_LOTE),
  não por posição de linha — assim um lote permanece estável entre execuções
  mesmo com registros novos sendo inseridos. A chave de partição de cada fonte
  está em CHAVES_PARTICAO (ex: id_compra, id_compra_item). Como os IDs são
  monotônicos e registros antigos não são alterados, lotes antigos têm hash
  estável pra sempre; só os lotes na ponta (IDs recentes) tendem a mudar.

Manifesto incremental (pncp_em_numeros_manifesto.csv):
  Hash SHA-256 por (tabela, lote), calculado sobre os bytes do parquet já
  serializado daquele lote. Lotes com hash inalterado não são regravados.
  pncp_em_numeros_alteracoes.csv lista (tabela, lote) alterados nesta execução
  — é o que o dbt usa como filtro incremental, igual ncm/nfe_cgu fazem com
  arquivo/período. O manifesto é sincronizado com o bucket para persistir
  entre ambientes.

Uso:
  python -m ingestion.pncp_em_numeros.extract
"""

import csv
import hashlib
import io
import logging
import shutil
from datetime import datetime
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from databricks import sql

import shared.configurar_logging as log
from shared.baixar_arquivo import baixar_arquivo_do_bucket
from shared.carregar_segredo import carregar_segredo
from shared.salvar_arquivo import salvar_arquivo_no_bucket

log.setup_logging()
logger = logging.getLogger(__name__)

SEGREDO_DATABRICKS = "databricks-cotin"
SEGREDO_BUCKET = "colibri-token-desenvolvedor"

# nome_logico -> (catalog, schema, tabela_ou_view)
FONTES = {
    #"compra":        ("raw", "geral", "faseexterna_compra"),
    #"item":          ("raw", "geral", "faseexterna_item"),
    #"participacao":  ("raw", "geral", "faseexterna_participacao"),
    #"proposta_item": ("raw", "geral", "faseexterna_proposta_item"),
    "agg_compra":    ("cotin_dlt_pncp", "bronze", "vw_agg_compra_bronze"),
    "agg_itens":     ("cotin_dlt_pncp", "bronze", "vw_agg_itens_bronze"),
    "agg_resultado": ("cotin_dlt_pncp", "bronze", "vw_agg_resultado_bronze"),
}

# nome_logico -> coluna usada pra particionar em lotes estáveis (bigint, única, sem nulo)
CHAVES_PARTICAO = {
    "agg_compra":    "id_compra",
    "agg_itens":     "id_compra_item",
    "agg_resultado": "id_compra_item_resultado",
}

TAMANHO_LOTE = 250_000  # faixa de ID por lote (não é contagem de linhas)

DIRETORIO_DADOS = Path("./dados")
DIRETORIO_SAIDA = DIRETORIO_DADOS / "pncp_em_numeros"
DIRETORIO_MANIFESTOS = DIRETORIO_DADOS / "manifestos"
DIRETORIO_ALTERACOES = DIRETORIO_DADOS / "alteracoes"

NOME_MANIFESTO = "pncp_em_numeros_manifesto.csv"
COLUNAS_MANIFESTO = ["tabela", "lote", "hash_sha256", "num_linhas", "extraido_em"]

NOME_ALTERACOES = "pncp_em_numeros_alteracoes.csv"
COLUNAS_ALTERACOES = ["tabela", "lote"]


def conectar_databricks():
    config = carregar_segredo(SEGREDO_DATABRICKS)
    return sql.connect(
        server_hostname=config["server_hostname"],
        http_path=config["http_path"],
        access_token=config["access_token"],
    )


def carregar_manifesto(caminho: Path) -> dict[tuple[str, int], dict]:
    if not caminho.exists():
        return {}
    with open(caminho, newline="", encoding="utf-8") as f:
        return {(r["tabela"], int(r["lote"])): r for r in csv.DictReader(f)}


def salvar_manifesto(caminho: Path, manifesto: dict[tuple[str, int], dict]) -> None:
    with open(caminho, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUNAS_MANIFESTO)
        w.writeheader()
        w.writerows(sorted(manifesto.values(), key=lambda e: (e["tabela"], int(e["lote"]))))


def salvar_alteracoes(caminho: Path, alteracoes: list[tuple[str, int]]) -> None:
    with open(caminho, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(COLUNAS_ALTERACOES)
        w.writerows(alteracoes)


def _particionar_por_lote(tabela_arrow: pa.Table, coluna_chave: str) -> dict[int, pa.Table]:
    """Divide a tabela em sub-tabelas por faixa de ID (lote = chave // TAMANHO_LOTE).

    Ordena pela chave e corta em blocos contíguos (slice é O(1) no Arrow,
    não copia dados) em vez de filtrar a tabela inteira uma vez por lote.
    """
    indices = pc.sort_indices(tabela_arrow, sort_keys=[(coluna_chave, "ascending")])
    ordenada = tabela_arrow.take(indices)
    chaves = pc.cast(ordenada.column(coluna_chave), pa.int64())
    lotes_np = pc.divide(chaves, pa.scalar(TAMANHO_LOTE, type=pa.int64())).to_numpy(zero_copy_only=False)

    n = len(lotes_np)
    mudanca = np.where(np.diff(lotes_np) != 0)[0] + 1
    limites = np.concatenate(([0], mudanca, [n]))

    particoes: dict[int, pa.Table] = {}
    for i in range(len(limites) - 1):
        inicio, fim = int(limites[i]), int(limites[i + 1])
        lote = int(lotes_np[inicio])
        particoes[lote] = ordenada.slice(inicio, fim - inicio)
    return particoes


def _tabela_para_parquet_bytes_e_hash(tabela_arrow: pa.Table) -> tuple[bytes, str]:
    buffer = io.BytesIO()
    pq.write_table(tabela_arrow, buffer)
    bytes_parquet = buffer.getvalue()
    return bytes_parquet, hashlib.sha256(bytes_parquet).hexdigest()


def resetar_dados_locais() -> None:
    """Apaga os parquets extraídos e o manifesto local (usado por --do-zero)"""
    shutil.rmtree(DIRETORIO_SAIDA, ignore_errors=True)
    (DIRETORIO_MANIFESTOS / NOME_MANIFESTO).unlink(missing_ok=True)
    (DIRETORIO_ALTERACOES / NOME_ALTERACOES).unlink(missing_ok=True)


def subir_manifesto() -> None:
    """Sobe o manifesto local pro bucket. Só deve ser chamado depois do dbt rodar com sucesso"""
    caminho_manifesto = DIRETORIO_MANIFESTOS / NOME_MANIFESTO
    if not caminho_manifesto.exists():
        return
    bucket = carregar_segredo(SEGREDO_BUCKET)["bucket_lake"]
    try:
        salvar_arquivo_no_bucket(str(caminho_manifesto), bucket, SEGREDO_BUCKET, NOME_MANIFESTO)
    except Exception as e:
        logger.warning(f"Não foi possível salvar manifesto no bucket: {e}")


def executar_ingestao() -> bool:
    """Retorna True se algum dado novo foi extraído, False se nada mudou"""
    DIRETORIO_SAIDA.mkdir(parents=True, exist_ok=True)
    DIRETORIO_MANIFESTOS.mkdir(parents=True, exist_ok=True)
    DIRETORIO_ALTERACOES.mkdir(parents=True, exist_ok=True)

    caminho_manifesto = DIRETORIO_MANIFESTOS / NOME_MANIFESTO
    caminho_alteracoes = DIRETORIO_ALTERACOES / NOME_ALTERACOES
    bucket = carregar_segredo(SEGREDO_BUCKET)["bucket_lake"]

    caminho_manifesto.unlink(missing_ok=True)
    try:
        baixar_arquivo_do_bucket(NOME_MANIFESTO, bucket, SEGREDO_BUCKET, str(caminho_manifesto))
        logger.info(f"Manifesto baixado do bucket: {bucket}/{NOME_MANIFESTO}")
    except Exception as e:
        logger.warning(f"Manifesto não encontrado no bucket, iniciando do zero: {e}")

    manifesto = carregar_manifesto(caminho_manifesto)
    alteracoes: list[tuple[str, int]] = []
    manifesto_modificado = False

    conn = conectar_databricks()
    try:
        cursor = conn.cursor()
        try:
            for nome_logico, (catalogo, schema, nome_objeto) in FONTES.items():
                coluna_chave = CHAVES_PARTICAO[nome_logico]
                logger.info(f"Consultando {catalogo}.{schema}.{nome_objeto}...")
                cursor.execute(f"SELECT * FROM {catalogo}.{schema}.{nome_objeto}")
                tabela_arrow = cursor.fetchall_arrow()

                particoes = _particionar_por_lote(tabela_arrow, coluna_chave)
                logger.info(f"{nome_logico}: {tabela_arrow.num_rows:,} linha(s) em {len(particoes)} lote(s)")

                for lote, sub_tabela in particoes.items():
                    bytes_parquet, hash_atual = _tabela_para_parquet_bytes_e_hash(sub_tabela)
                    caminho_parquet = DIRETORIO_SAIDA / f"{nome_logico}_lote_{lote:04d}.parquet"

                    entrada = manifesto.get((nome_logico, lote))
                    if entrada and entrada["hash_sha256"] == hash_atual and caminho_parquet.exists():
                        continue

                    caminho_parquet.write_bytes(bytes_parquet)
                    manifesto[(nome_logico, lote)] = {
                        "tabela": nome_logico,
                        "lote": lote,
                        "hash_sha256": hash_atual,
                        "num_linhas": sub_tabela.num_rows,
                        "extraido_em": datetime.now().isoformat(timespec="seconds"),
                    }
                    alteracoes.append((nome_logico, lote))
                    manifesto_modificado = True

                lotes_desta_fonte = [lote for fonte, lote in alteracoes if fonte == nome_logico]
                if lotes_desta_fonte:
                    logger.info(f"{nome_logico}: {len(lotes_desta_fonte)} lote(s) alterado(s): {lotes_desta_fonte}")
                else:
                    logger.info(f"{nome_logico}: nenhum lote alterado")
        finally:
            cursor.close()
    finally:
        conn.close()

    salvar_manifesto(caminho_manifesto, manifesto)
    salvar_alteracoes(caminho_alteracoes, alteracoes)
    logger.info(f"Manifesto: {caminho_manifesto} ({len(manifesto)} lote(s) no total)")
    logger.info(f"Alterações: {caminho_alteracoes} ({len(alteracoes)} lote(s) alterado(s))")

    return manifesto_modificado


if __name__ == "__main__":
    executar_ingestao()
