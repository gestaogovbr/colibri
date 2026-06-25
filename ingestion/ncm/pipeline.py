import os
import subprocess
from pathlib import Path

from ingestion.ncm.extract import executar_ingestao, resetar_dados_locais, subir_manifesto
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
                "--select", "stg_ncm int_ncm int_ncm_prefixos stg_dim_margem_ncm",
                "--project-dir", str(DBT_DIR),
                "--profiles-dir", str(DBT_DIR),
            ],
            cwd=str(DBT_DIR),
            check=True,
        )
    else:
        print("[dbt] Sem dados novos na extração, pulando dbt run.")

    subir_manifesto()
    subir_catalogo_simples(config, bucket)


if __name__ == "__main__":
    main()
