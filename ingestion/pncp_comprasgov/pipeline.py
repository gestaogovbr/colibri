import os
import subprocess

from ingestion.pncp_comprasgov.extract import (
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
    houve_mudanca = executar_ingestao(bucket_nome=bucket)

    if houve_mudanca:
        subprocess.run(
            [
                "dbt",
                "run",
                "--select",
                "stg_pncp_comprasgov__compras int_pncp_comprasgov__compras mrt_pncp_comprasgov_compras "
                "stg_pncp_comprasgov__itens int_pncp_comprasgov__itens mrt_pncp_comprasgov_itens "
                "stg_pncp_comprasgov__resultados int_pncp_comprasgov__resultados mrt_pncp_comprasgov_resultados",
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
        )
    else:
        print("[dbt] Sem dados novos na extração, pulando dbt run.")

    # Só sobe manifesto/catálogo se chegou até aqui: se o dbt quebrar, a exceção
    # interrompe a função antes disso, e o bucket fica intacto pra próxima tentativa.
    subir_manifesto(bucket_nome=bucket)
    salvar_arquivo_no_bucket(CATALOGO_LOCAL, bucket, NOME_SEGREDO_DESENVOLVEDOR, CATALOGO_LOCAL)


if __name__ == "__main__":
    main()
