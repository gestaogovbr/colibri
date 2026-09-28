"""
Pipeline dos cabeçalhos de contratações da API pública do PNCP.

Ordem: extrai o período (API) -> sobe os parquets para o bucket -> roda o dbt só
nos modelos desta fonte -> sincroniza o catálogo. É a mesma coreografia dos outros
pipelines do projeto; a diferença é que aqui o período é um argumento, porque a
varredura completa (~84 h, ver docs/nota-pncp-api.md) não cabe numa execução só.

Uso:
  python -m ingestion.pncp.pipeline --ano 2026 --mes 8
"""

import argparse
import logging
import os
import subprocess

from ingestion.pncp.extract import DIRETORIO_SAIDA, MODALIDADES, executar
from utils.baixar_catalogo import baixar_catalogo
from utils.carregar_segredo import carregar_segredo
from utils.constantes import (
    BUCKET_PRODUCAO,
    CATALOGO_LOCAL,
    DBT_DIR,
    NOME_SEGREDO_DESENVOLVEDOR,
    RAIZ_PROJETO,
)
from utils.criar_cliente import criar_cliente
from utils.salvar_arquivo_no_bucket import salvar_arquivo_no_bucket

logger = logging.getLogger(__name__)

MODELOS_DBT = "stg_pncp__contratacoes"


def main(
    ano: int, mes: int, eixo: str = "atualizacao", modalidades: list[int] | None = None, bucket: str | None = None
):
    os.chdir(RAIZ_PROJETO)
    bucket = bucket or BUCKET_PRODUCAO
    modalidades = modalidades or list(MODALIDADES)

    config = carregar_segredo(NOME_SEGREDO_DESENVOLVEDOR)
    cliente = criar_cliente(config)
    baixar_catalogo(cliente, bucket, CATALOGO_LOCAL)

    metricas = executar(ano, mes, eixo, modalidades)
    if not metricas["arquivos"]:
        logger.info("[pncp] Nada novo no período, pulando dbt.")
        return metricas

    for arquivo in metricas["arquivos"]:
        nome = f"pncp-{eixo}-{arquivo['modalidade']}-{arquivo['periodo']}.parquet"
        salvar_arquivo_no_bucket(str(DIRETORIO_SAIDA / nome), bucket, NOME_SEGREDO_DESENVOLVEDOR, f"pncp/{nome}")

    subprocess.run(
        [
            "dbt",
            "run",
            "--select",
            MODELOS_DBT,
            "--vars",
            f"bucket_lake: {bucket}",
            "--project-dir",
            str(DBT_DIR),
            "--profiles-dir",
            str(DBT_DIR),
            "--target",
            "prod",
        ],
        cwd=str(DBT_DIR),
        check=True,
        env={**os.environ, "DBT_BUCKET_LAKE": bucket},
    )

    salvar_arquivo_no_bucket(CATALOGO_LOCAL, bucket, NOME_SEGREDO_DESENVOLVEDOR, CATALOGO_LOCAL)
    return metricas


if __name__ == "__main__":
    analisador = argparse.ArgumentParser(description="Pipeline de cabeçalhos do PNCP")
    analisador.add_argument("--ano", type=int, required=True)
    analisador.add_argument("--mes", type=int, required=True)
    analisador.add_argument("--eixo", choices=["atualizacao", "publicacao"], default="atualizacao")
    analisador.add_argument("--bucket", default=None)
    argumentos = analisador.parse_args()
    main(argumentos.ano, argumentos.mes, argumentos.eixo, bucket=argumentos.bucket)
