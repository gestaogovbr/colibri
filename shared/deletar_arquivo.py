import boto3
import click
from botocore.exceptions import ClientError

import shared.configurar_logging as log
from shared.carregar_segredo import carregar_segredo


class ArquivoNaoEncontradoError(Exception):
    pass


@click.command()
@click.argument("nome_arquivo")
@click.argument("bucket_name")
@click.argument("nome_segredo")
def deletar_arquivo(nome_arquivo: str, bucket_name: str, nome_segredo: str):
    config = carregar_segredo(nome_segredo)
    cliente = boto3.client(
        "s3",
        endpoint_url=config["endpoint"],
        aws_access_key_id=config["access_key"],
        aws_secret_access_key=config["secret_key"],
        region_name="auto",
    )

    try:
        cliente.head_object(Bucket=bucket_name, Key=nome_arquivo)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            raise ArquivoNaoEncontradoError(
                log.ARQUIVO_NAO_ENCONTRADO_NO_BUCKET % (nome_arquivo, bucket_name)
            ) from e
        raise

    cliente.delete_object(Bucket=bucket_name, Key=nome_arquivo)
    print(f"Deletado: {nome_arquivo}")


if __name__ == "__main__":
    deletar_arquivo()