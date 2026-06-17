import os
import subprocess
from pathlib import Path

import boto3
import duckdb

from ingestion.pncp_comprasgov.extract import executar_ingestao
from shared.carregar_segredo import carregar_segredo

_RAIZ = Path(__file__).resolve().parent.parent.parent
_DBT_DIR = _RAIZ / "dbt"
_CATALOGO_LOCAL = _RAIZ / "meta.ducklake"
_CHAVE_CATALOGO = "meta.ducklake"
_SEGREDO_NOME = "colibri-token-desenvolvedor"

NOME_SEGREDO = _SEGREDO_NOME
CAMINHO_META  = f"s3://colibri-dev/{_CHAVE_CATALOGO}"
DATA_PATH     = "s3://colibri-dev/lake/"


def _cliente_s3(config: dict):
    return boto3.client(
        "s3",
        endpoint_url=config["endpoint"],
        aws_access_key_id=config["access_key"],
        aws_secret_access_key=config["secret_key"],
        region_name="auto",
    )


def _baixar_ou_criar_catalogo(config: dict, bucket: str) -> None:
    s3 = _cliente_s3(config)
    try:
        s3.download_file(bucket, _CHAVE_CATALOGO, str(_CATALOGO_LOCAL))
        print(f"[ducklake] Catálogo baixado de s3://{bucket}/{_CHAVE_CATALOGO}")
    except Exception:
        _CATALOGO_LOCAL.unlink(missing_ok=True)
        con = duckdb.connect()
        con.install_extension("ducklake")
        con.load_extension("ducklake")
        con.execute(f"ATTACH 'ducklake:{_CATALOGO_LOCAL}' AS lake (DATA_PATH 's3://{bucket}/lake/')")
        con.close()
        print(f"[ducklake] Novo catálogo criado em {_CATALOGO_LOCAL}")


def _subir_catalogo(config: dict, bucket: str) -> None:
    s3 = _cliente_s3(config)
    s3.upload_file(str(_CATALOGO_LOCAL), bucket, _CHAVE_CATALOGO)
    print(f"[ducklake] Catálogo sincronizado para s3://{bucket}/{_CHAVE_CATALOGO}")


def main():
    os.chdir(_RAIZ)
    config = carregar_segredo(_SEGREDO_NOME)
    bucket = config["bucket_lake"]

    _baixar_ou_criar_catalogo(config, bucket)
    executar_ingestao()

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

    _subir_catalogo(config, bucket)


if __name__ == "__main__":
    main()
