import os
import subprocess

from ingestion.nfe_cgu.extract import (
    executar_ingestao,
    subir_manifesto,
)
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


def main(bucket: str | None = None):
    os.chdir(RAIZ_PROJETO)
    bucket = bucket or BUCKET_PRODUCAO
    config = carregar_segredo(NOME_SEGREDO_DESENVOLVEDOR)
    cliente = criar_cliente(config)

    baixar_catalogo(cliente, bucket, CATALOGO_LOCAL)
    resultado = executar_ingestao(bucket=bucket)

    if resultado.houve_mudanca:
        # Append sobre tabela com tipos antigos duplicaria os períodos e, sem o
        # on_schema_change='fail', converteria a chave de volta para DOUBLE (#98).
        refazer = ["--full-refresh"] if resultado.refazer_staging else []
        subprocess.run(
            [
                "dbt",
                "run",
                *refazer,
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
    salvar_arquivo_no_bucket(CATALOGO_LOCAL, bucket, NOME_SEGREDO_DESENVOLVEDOR, CATALOGO_LOCAL)


if __name__ == "__main__":
    main()
