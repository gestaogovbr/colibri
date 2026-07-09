"""
Extração das tabelas/views "PNCP em números" via Databricks SQL.

Lê 4 tabelas brutas (catalog raw, schema geral) e 3 views agregadas (catalog
cotin_dlt_pncp, schema bronze) e salva o DELTA (linhas novas ou alteradas) de
cada uma como parquet em dados/pncp_em_numeros/.

Diff por linha (CHAVES_UNICAS):
  Essas fontes têm registros que são ATUALIZADOS depois de criados (ex:
  situacao_compra muda) -- não é só INSERT de linha nova, por isso não dá pra
  usar um watermark de ID puro. Cada execução busca a tabela inteira, mas:
    1. Compara o hash SHA-256 da tabela inteira com o da última execução
       (manifesto). Se bater, não faz mais nada -- atalho barato pro caso
       comum (nada mudou).
    2. Se o hash mudou, compara LINHA A LINHA contra o snapshot completo da
       execução anterior (dados/manifestos/pncp_em_numeros_snapshot_<fonte>.parquet),
       via hash vetorizado (pandas.util.hash_pandas_object, dtype UInt64 pra
       não perder precisão no diff). Só as linhas novas ou com hash diferente
       entram no delta.
  O delta vai pra um parquet novo (nome com timestamp) -- é só ele que o dbt
  lê (incremental_strategy='append'). O snapshot completo é só uso interno,
  pra comparação na próxima execução; não é lido pelo dbt.

Manifesto (pncp_em_numeros_manifesto.csv): tabela, hash_tabela, num_linhas,
  extraido_em.
Alterações (pncp_em_numeros_alteracoes.csv): tabela, arquivo -- lista os
  arquivos de delta desta execução, usada pelo dbt como filtro incremental,
  igual ncm/nfe_cgu fazem com arquivo/período.

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

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from databricks import sql

import utils.configurar_logging as log
from utils.baixar_arquivo import baixar_arquivo_do_bucket
from utils.carregar_segredo import carregar_segredo
from utils.constantes import NOME_SEGREDO_DESENVOLVEDOR
from utils.manifesto_bucket import baixar_manifesto, subir_manifesto as _subir_manifesto
from utils.salvar_arquivo_timestamp import salvar_arquivo_no_bucket

log.setup_logging()
logger = logging.getLogger(__name__)

SEGREDO_DATABRICKS = "databricks-cotin"

# nome_logico -> (catalog, schema, tabela_ou_view)
FONTES = {
    # "compra":        ("raw", "geral", "faseexterna_compra"),
    # "item":          ("raw", "geral", "faseexterna_item"),
    # "participacao":  ("raw", "geral", "faseexterna_participacao"),
    # "proposta_item": ("raw", "geral", "faseexterna_proposta_item"),
    "agg_compra": ("cotin_dlt_pncp", "bronze", "vw_agg_compra_bronze"),
    "agg_itens": ("cotin_dlt_pncp", "bronze", "vw_agg_itens_bronze"),
    "agg_resultado": ("cotin_dlt_pncp", "bronze", "vw_agg_resultado_bronze"),
}

# nome_logico -> coluna chave única (bigint, sem nulo) usada no diff linha a linha
CHAVES_UNICAS = {
    "agg_compra": "id_compra",
    "agg_itens": "id_compra_item",
    "agg_resultado": "id_compra_item_resultado",
}

DIRETORIO_DADOS = Path("./dados")
DIRETORIO_SAIDA = DIRETORIO_DADOS / "pncp_em_numeros"
DIRETORIO_MANIFESTOS = DIRETORIO_DADOS / "manifestos"
DIRETORIO_ALTERACOES = DIRETORIO_DADOS / "alteracoes"

NOME_MANIFESTO = "pncp_em_numeros_manifesto.csv"
COLUNAS_MANIFESTO = ["tabela", "hash_tabela", "num_linhas", "extraido_em"]

NOME_ALTERACOES = "pncp_em_numeros_alteracoes.csv"
COLUNAS_ALTERACOES = ["tabela", "arquivo"]


def conectar_databricks():
    config = carregar_segredo(SEGREDO_DATABRICKS)
    return sql.connect(
        server_hostname=config["server_hostname"],
        http_path=config["http_path"],
        access_token=config["access_token"],
    )


def carregar_manifesto(caminho: Path) -> dict[str, dict]:
    if not caminho.exists():
        return {}
    with open(caminho, newline="", encoding="utf-8") as f:
        return {r["tabela"]: r for r in csv.DictReader(f)}


def salvar_manifesto(caminho: Path, manifesto: dict[str, dict]) -> None:
    with open(caminho, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUNAS_MANIFESTO)
        w.writeheader()
        w.writerows(sorted(manifesto.values(), key=lambda e: e["tabela"]))


def salvar_alteracoes(caminho: Path, alteracoes: list[tuple[str, str]]) -> None:
    with open(caminho, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(COLUNAS_ALTERACOES)
        w.writerows(alteracoes)


def _caminho_snapshot(nome_logico: str) -> Path:
    return DIRETORIO_MANIFESTOS / f"pncp_em_numeros_snapshot_{nome_logico}.parquet"


def _tabela_para_parquet_bytes_e_hash(tabela_arrow: pa.Table) -> tuple[bytes, str]:
    buffer = io.BytesIO()
    pq.write_table(tabela_arrow, buffer)
    bytes_parquet = buffer.getvalue()
    return bytes_parquet, hashlib.sha256(bytes_parquet).hexdigest()


def _linhas_novas_ou_alteradas(
    df_novo: pd.DataFrame, df_velho: pd.DataFrame, chave: str
) -> pd.DataFrame:
    """Compara linha a linha (hash vetorizado) e devolve só as linhas de df_novo
    que são novas (chave ausente em df_velho) ou tiveram conteúdo alterado"""
    hash_novo = pd.util.hash_pandas_object(
        df_novo.drop(columns=[chave]), index=False
    ).astype("UInt64")
    serie_nova = pd.Series(
        hash_novo.values, index=df_novo[chave].values, dtype="UInt64"
    )

    if df_velho.empty:
        mascara = pd.Series(True, index=df_novo.index)
    else:
        hash_velho = pd.util.hash_pandas_object(
            df_velho.drop(columns=[chave]), index=False
        ).astype("UInt64")
        serie_velha = pd.Series(
            hash_velho.values, index=df_velho[chave].values, dtype="UInt64"
        )
        alinhado = serie_velha.reindex(serie_nova.index)
        mascara = pd.Series(
            (serie_nova.values != alinhado.values).to_numpy(dtype=bool, na_value=True),
            index=df_novo.index,
        )

    return df_novo[mascara]


def resetar_dados_locais() -> None:
    """Apaga os parquets extraídos, os snapshots de referência e o manifesto local (usado por --do-zero)"""
    shutil.rmtree(DIRETORIO_SAIDA, ignore_errors=True)
    for nome_logico in CHAVES_UNICAS:
        _caminho_snapshot(nome_logico).unlink(missing_ok=True)
    (DIRETORIO_MANIFESTOS / NOME_MANIFESTO).unlink(missing_ok=True)
    (DIRETORIO_ALTERACOES / NOME_ALTERACOES).unlink(missing_ok=True)


def subir_manifesto() -> None:
    """Sobe o manifesto e os snapshots de referência pro bucket. Só deve ser
    chamado depois do dbt rodar com sucesso"""
    bucket = carregar_segredo(SEGREDO_BUCKET_DESENVOLVEDOR)["bucket_lake"]
    _subir_manifesto(
        DIRETORIO_MANIFESTOS / NOME_MANIFESTO,
        NOME_MANIFESTO,
        bucket,
        SEGREDO_BUCKET_DESENVOLVEDOR,
        logger,
    )
    for nome_logico in FONTES:
        caminho = _caminho_snapshot(nome_logico)
        if caminho.exists():
            try:
                salvar_arquivo_no_bucket(
                    str(caminho), bucket, SEGREDO_BUCKET_DESENVOLVEDOR, caminho.name
                )
            except Exception as e:
                logger.warning(
                    f"Não foi possível salvar snapshot {caminho.name} no bucket: {e}"
                )


def executar_ingestao() -> bool:
    """Retorna True se algum dado novo foi extraído, False se nada mudou"""
    DIRETORIO_SAIDA.mkdir(parents=True, exist_ok=True)
    DIRETORIO_MANIFESTOS.mkdir(parents=True, exist_ok=True)
    DIRETORIO_ALTERACOES.mkdir(parents=True, exist_ok=True)

    caminho_manifesto = DIRETORIO_MANIFESTOS / NOME_MANIFESTO
    caminho_alteracoes = DIRETORIO_ALTERACOES / NOME_ALTERACOES
    bucket = carregar_segredo(SEGREDO_BUCKET_DESENVOLVEDOR)["bucket_lake"]

    baixar_manifesto(caminho_manifesto, NOME_MANIFESTO, bucket, SEGREDO_BUCKET_DESENVOLVEDOR, logger)
    manifesto = carregar_manifesto(caminho_manifesto)
    alteracoes: list[tuple[str, str]] = []
    manifesto_modificado = False

    conn = conectar_databricks()
    try:
        cursor = conn.cursor()
        try:
            for nome_logico, (catalogo, schema, nome_objeto) in FONTES.items():
                chave = CHAVES_UNICAS[nome_logico]
                caminho_snapshot = _caminho_snapshot(nome_logico)

                # baixa o snapshot de referência da execução anterior, se existir no bucket
                try:
                    baixar_arquivo_do_bucket(
                        caminho_snapshot.name,
                        bucket,
                        SEGREDO_BUCKET_DESENVOLVEDOR,
                        str(caminho_snapshot),
                    )
                except Exception:
                    pass  # primeira execução, ou snapshot ainda não existe -- segue sem ele

                logger.info(f"Consultando {catalogo}.{schema}.{nome_objeto}...")
                cursor.execute(f"SELECT * FROM {catalogo}.{schema}.{nome_objeto}")
                tabela_arrow = cursor.fetchall_arrow()
                bytes_parquet, hash_atual = _tabela_para_parquet_bytes_e_hash(
                    tabela_arrow
                )

                entrada = manifesto.get(nome_logico)
                if (
                    entrada
                    and entrada["hash_tabela"] == hash_atual
                    and caminho_snapshot.exists()
                ):
                    logger.info(
                        f"{nome_logico}: hash da tabela bate com manifesto, nada mudou"
                    )
                    continue

                df_novo = tabela_arrow.to_pandas()
                df_velho = (
                    pd.read_parquet(caminho_snapshot)
                    if caminho_snapshot.exists()
                    else df_novo.iloc[0:0]
                )
                delta = _linhas_novas_ou_alteradas(df_novo, df_velho, chave)

                if delta.empty:
                    logger.info(
                        f"{nome_logico}: hash da tabela mudou mas nenhuma linha nova/alterada (provável reordenação)"
                    )
                else:
                    agora = datetime.now()
                    nome_arquivo = (
                        f"{nome_logico}_{agora.strftime('%Y-%m-%d-%H%M%S')}.parquet"
                    )
                    pq.write_table(
                        pa.Table.from_pandas(delta, preserve_index=False),
                        DIRETORIO_SAIDA / nome_arquivo,
                    )
                    alteracoes.append((nome_logico, nome_arquivo))
                    logger.info(
                        f"{nome_logico}: {len(delta):,} linha(s) nova(s)/alterada(s) em {nome_arquivo}"
                    )

                # snapshot completo sempre atualizado, mesmo se o delta ficou vazio
                # (cobre o caso de hash da tabela diferente por motivo que não afeta conteúdo por linha)
                caminho_snapshot.write_bytes(bytes_parquet)

                manifesto[nome_logico] = {
                    "tabela": nome_logico,
                    "hash_tabela": hash_atual,
                    "num_linhas": tabela_arrow.num_rows,
                    "extraido_em": datetime.now().isoformat(timespec="seconds"),
                }
                manifesto_modificado = True
        finally:
            cursor.close()
    finally:
        conn.close()

    salvar_manifesto(caminho_manifesto, manifesto)
    salvar_alteracoes(caminho_alteracoes, alteracoes)
    logger.info(f"Manifesto: {caminho_manifesto} ({len(manifesto)} fonte(s))")
    logger.info(
        f"Alterações: {caminho_alteracoes} ({len(alteracoes)} arquivo(s) novo(s))"
    )

    return manifesto_modificado


if __name__ == "__main__":
    executar_ingestao()
