import os
from pathlib import Path

import boto3
import duckdb

from ingestion.pncp_em_numeros.extract import executar_ingestao, resetar_dados_locais, subir_manifesto
from shared.carregar_segredo import carregar_segredo

_RAIZ = Path(__file__).resolve().parent.parent.parent
_CATALOGO_LOCAL = _RAIZ / "meta.ducklake"
_CHAVE_CATALOGO = "meta.ducklake"
_SEGREDO_NOME = "colibri-token-desenvolvedor"


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
    houve_mudanca = executar_ingestao()

    if houve_mudanca:
        print("[dbt] Dados novos extraídos, mas ainda não há modelo dbt pra essa fonte.")
    else:
        print("[dbt] Sem dados novos na extração.")

    subir_manifesto()
    _subir_catalogo(config, bucket)


if __name__ == "__main__":
    main()
