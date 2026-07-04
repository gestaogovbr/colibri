import os
import subprocess

from ingestion.pncp_em_numeros.extract import (
    executar_ingestao,
    resetar_dados_locais,
    subir_manifesto,
)
from utils.carregar_segredo import carregar_segredo
from utils.criar_cliente import criar_cliente
from utils.baixar_catalogo import baixar_catalogo
from utils.salvar_arquivo_no_bucket import salvar_arquivo_no_bucket
from utils.constantes import BUCKET, CATALOGO_LOCAL, DBT_DIR, RAIZ_PROJETO, NOME_SEGREDO


def main():
    os.chdir(RAIZ_PROJETO)
    config = carregar_segredo(NOME_SEGREDO)
    cliente = criar_cliente(config)

    baixar_catalogo(cliente, BUCKET, CATALOGO_LOCAL)
    houve_mudanca = executar_ingestao()

    if houve_mudanca:
        subprocess.run(
            [
                "dbt",
                "run",
                "--select",
                "stg_pncp_em_numeros__agg_compra int_pncp_em_numeros__agg_compra",
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

    # Só sobe manifesto/catálogo se chegou até aqui: se o dbt quebrar, a exceção
    # interrompe a função antes disso, e o bucket fica intacto pra próxima tentativa.
    subir_manifesto()
    salvar_arquivo_no_bucket(CATALOGO_LOCAL, BUCKET, NOME_SEGREDO, CATALOGO_LOCAL)


if __name__ == "__main__":
    main()
