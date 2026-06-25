import boto3
import click
from botocore.exceptions import ClientError

import utils.configurar_logging as log
from utils.carregar_segredo import carregar_segredo


class ArquivoNaoEncontradoError(Exception):
    pass


def _criar_cliente(nome_segredo: str):
    config = carregar_segredo(nome_segredo)
    return boto3.client(
        "s3",
        endpoint_url=config["endpoint"],
        aws_access_key_id=config["access_key"],
        aws_secret_access_key=config["secret_key"],
        region_name="auto",
    )


@click.command()
@click.argument("nome_arquivo")
@click.argument("bucket_name")
@click.argument("nome_segredo")
def deletar_arquivo(nome_arquivo: str, bucket_name: str, nome_segredo: str):
    cliente = _criar_cliente(nome_segredo)

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


@click.command()
@click.argument("bucket_name")
@click.argument("nome_segredo")
@click.option("--prefixo", default="", help="Limitar a um prefixo específico")
@click.confirmation_option(prompt="Isso vai deletar todos os objetos. Confirma?")
def deletar_tudo(bucket_name: str, nome_segredo: str, prefixo: str):
    cliente = _criar_cliente(nome_segredo)

    paginator = cliente.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefixo)

    objetos = [{"Key": obj["Key"]} for page in pages for obj in page.get("Contents", [])]

    if not objetos:
        print("Bucket vazio. Nada a deletar.")
        return

    for i in range(0, len(objetos), 1000):
        lote = objetos[i:i + 1000]
        cliente.delete_objects(Bucket=bucket_name, Delete={"Objects": lote})

    print(f"{len(objetos)} objeto(s) deletado(s) de '{bucket_name}'.")


if __name__ == "__main__":
    deletar_tudo()
