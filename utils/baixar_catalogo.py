import os

from botocore.exceptions import ClientError

from utils.constantes import CATALOGO_LOCAL


def baixar_catalogo(cliente, bucket: str, chave: str, destino: str = CATALOGO_LOCAL):
    try:
        cliente.download_file(bucket, chave, destino)
        print(f"[ducklake] Catálogo baixado de s3://{bucket}/{chave}")
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            print("[ducklake] Catálogo não existe no R2. Será criado.")
            if os.path.exists(destino):
                os.remove(destino)
        else:
            raise
