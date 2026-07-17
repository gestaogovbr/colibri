import os
import subprocess

from utils.carregar_segredo import carregar_segredo
from utils.criar_cliente import criar_cliente
from utils.baixar_catalogo import baixar_catalogo
from utils.salvar_arquivo_no_bucket import salvar_arquivo_no_bucket
from utils.constantes import (
    BUCKET_PRODUCAO,
    CATALOGO_LOCAL,
    DBT_DIR,
    RAIZ_PROJETO,
    NOME_SEGREDO_DESENVOLVEDOR,
)

MODELOS_DBT = "stg_margem__resolucoes stg_margem__eventos int_margem__ncm_prefixos mrt_margem__ncms_cics mrt_margem__ncms_ciiapac"


def main(bucket: str | None = None):
    """Sem extração: os modelos leem a planilha de resoluções direto do bucket colibri-arquivos"""
    os.chdir(RAIZ_PROJETO)
    bucket = bucket or BUCKET_PRODUCAO
    config = carregar_segredo(NOME_SEGREDO_DESENVOLVEDOR)
    cliente = criar_cliente(config)

    baixar_catalogo(cliente, bucket, CATALOGO_LOCAL)

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

    salvar_arquivo_no_bucket(
        CATALOGO_LOCAL, bucket, NOME_SEGREDO_DESENVOLVEDOR, CATALOGO_LOCAL
    )


if __name__ == "__main__":
    main()
