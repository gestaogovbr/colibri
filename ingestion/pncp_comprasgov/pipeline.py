import os
import subprocess
from pathlib import Path

from ingestion.pncp_comprasgov.extract import executar_ingestao, resetar_dados_locais, subir_manifesto
from utils.carregar_segredo import carregar_segredo
from utils.ducklake import baixar_ou_criar_catalogo, subir_catalogo_simples

_RAIZ = Path(__file__).resolve().parent.parent.parent
_DBT_DIR = _RAIZ / "dbt"
_SEGREDO_NOME = "colibri-token-desenvolvedor"

NOME_SEGREDO = _SEGREDO_NOME
CAMINHO_META = "s3://colibri-dev/meta.ducklake"
DATA_PATH = "s3://colibri-dev/lake/"


def main():
    os.chdir(_RAIZ)
    config = carregar_segredo(_SEGREDO_NOME)
    bucket = config["bucket_lake"]

    baixar_ou_criar_catalogo(config, bucket)
    houve_mudanca = executar_ingestao()

    if houve_mudanca:
        subprocess.run(
            [
                "dbt", "run",
                "--select",
                "stg_pncp_comprasgov__compras int_pncp_comprasgov__compras mrt_pncp_comprasgov__resumo_anual stg_pncp_comprasgov__itens int_pncp_comprasgov__itens stg_pncp_comprasgov__resultados int_pncp_comprasgov__resultados",
                "--project-dir", str(_DBT_DIR),
                "--profiles-dir", str(_DBT_DIR),
            ],
            cwd=str(_DBT_DIR),
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