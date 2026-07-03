import os
import subprocess

from ingestion.margem_preferencia.extract import executar_ingestao, resetar_dados_locais, subir_manifesto
from utils.carregar_segredo import carregar_segredo
from utils.criar_cliente import criar_cliente
from utils.baixar_catalogo import baixar_catalogo
from utils.salvar_arquivo_no_bucket import salvar_arquivo_no_bucket
from utils.constantes import BUCKET, CATALOGO_LOCAL, DBT_DIR, RAIZ_PROJETO, NOME_SEGREDO

MODELOS_DBT = "int_dim_margem_resolucoes stg_fato_margem_eventos stg_dim_margem_ncm_prefixos stg_dim_margem_ncm"


def main(bucket: str | None = None):
    os.chdir(RAIZ_PROJETO)
    bucket = bucket or BUCKET
    config = carregar_segredo(NOME_SEGREDO)
    cliente = criar_cliente(config)

    baixar_catalogo(cliente, bucket, CATALOGO_LOCAL)
    houve_mudanca = executar_ingestao(bucket=bucket)

    if houve_mudanca:
        subprocess.run(
            [
                "dbt", "run",
                "--select", MODELOS_DBT,
                "--project-dir", str(DBT_DIR),
                "--profiles-dir", str(DBT_DIR),
            ],
            cwd=str(DBT_DIR),
            check=True,
        )
    else:
        print("[dbt] Sem dados novos, pulando dbt run.")

    subir_manifesto(bucket=bucket)
    salvar_arquivo_no_bucket(CATALOGO_LOCAL, bucket, NOME_SEGREDO, CATALOGO_LOCAL)


if __name__ == "__main__":
    main()
