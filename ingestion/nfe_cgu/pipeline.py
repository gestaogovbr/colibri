import os
import subprocess

from ingestion.nfe_cgu.extract import (
    executar_ingestao,
    subir_manifesto,
)
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


def main(bucket: str | None = None):
    os.chdir(RAIZ_PROJETO)
    bucket = bucket or BUCKET_PRODUCAO
    config = carregar_segredo(NOME_SEGREDO_DESENVOLVEDOR)
    cliente = criar_cliente(config)

    baixar_catalogo(cliente, bucket, CATALOGO_LOCAL)
    houve_mudanca = executar_ingestao(bucket=bucket)

    if houve_mudanca:
        subprocess.run(
            [
                "dbt",
                "run",
                "--select",
                "stg_nfe_cgu__itens stg_nfe_cgu__nf stg_nfe_cgu__eventos",
                "--vars",
                f"bucket_lake: {bucket}",
                "--project-dir",
                str(DBT_DIR),
                "--profiles-dir",
                str(DBT_DIR),
            ],
            cwd=str(DBT_DIR),
            check=True,
        )
    else:
        print("[dbt] Sem dados novos na extração, pulando dbt run.")

    subir_manifesto(bucket=bucket)
    salvar_arquivo_no_bucket(
        CATALOGO_LOCAL, bucket, NOME_SEGREDO_DESENVOLVEDOR, CATALOGO_LOCAL
    )


if __name__ == "__main__":
    main()
