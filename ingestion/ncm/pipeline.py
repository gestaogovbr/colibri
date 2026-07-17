import os
import subprocess

from ingestion.ncm.extract import executar_ingestao
from utils.carregar_segredo import carregar_segredo
from utils.criar_cliente import criar_cliente
from utils.baixar_catalogo import baixar_catalogo
from utils.salvar_arquivo_no_bucket import salvar_arquivo_no_bucket
from utils.constantes import BUCKET_PRODUCAO, CATALOGO_LOCAL, DBT_DIR, RAIZ_PROJETO, NOME_SEGREDO_DESENVOLVEDOR


def main(bucket: str | None = None):
    os.chdir(RAIZ_PROJETO)
    bucket = bucket or BUCKET_PRODUCAO
    config = carregar_segredo(NOME_SEGREDO_DESENVOLVEDOR)
    cliente = criar_cliente(config)

    baixar_catalogo(cliente, bucket, CATALOGO_LOCAL)
    executar_ingestao(bucket=bucket)

    subprocess.run(
        [
            "dbt",
            "run",
            "--select",
            "int_ncm__prefixos int_ncm__tradutor_prefixo_subitem mrt_ncm__codigos",
            "--vars",
            f"bucket_lake: {bucket}",
            "--project-dir",
            str(DBT_DIR),
            "--profiles-dir",
            str(DBT_DIR),
        ],
        cwd=str(DBT_DIR),
        check=True,
        env={**os.environ, "DBT_BUCKET_LAKE": bucket},
    )

    salvar_arquivo_no_bucket(CATALOGO_LOCAL, bucket, NOME_SEGREDO_DESENVOLVEDOR, CATALOGO_LOCAL)


if __name__ == "__main__":
    main()
