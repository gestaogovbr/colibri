import os
import subprocess
from pathlib import Path

from ingestion.pncp_em_numeros.extract import executar_ingestao, resetar_dados_locais, subir_manifesto
from utils.carregar_segredo import carregar_segredo
from utils.ducklake import baixar_ou_criar_catalogo, subir_catalogo_simples

_RAIZ = Path(__file__).resolve().parent.parent.parent
DBT_DIR = _RAIZ / "dbt"
NOME_SEGREDO = "colibri-token-desenvolvedor"


def main():
    os.chdir(_RAIZ)
    config = carregar_segredo(NOME_SEGREDO)
    bucket = config["bucket_lake"]

    baixar_ou_criar_catalogo(config, bucket)
    houve_mudanca = executar_ingestao()

    if houve_mudanca:
        subprocess.run(
            [
                "dbt", "run",
                "--select",
                "stg_pncp_em_numeros__agg_compra int_pncp_em_numeros__agg_compra",
                "--project-dir", str(DBT_DIR),
                "--profiles-dir", str(DBT_DIR),
            ],
            cwd=str(DBT_DIR),
            check=True,
        )
    else:
        print("[dbt] Sem dados novos na extração, pulando dbt run.")

    # Só sobe manifesto/catálogo se chegou até aqui: se o dbt quebrar, a exceção
    # interrompe a função antes disso, e o bucket fica intacto pra próxima tentativa.
    subir_manifesto()
    subir_catalogo_simples(config, bucket)


if __name__ == "__main__":
    main()
