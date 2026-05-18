import os

import boto3
import click

from shared.carregar_segredo import carregar_segredo


@click.command()
@click.argument("nome_arquivo")
@click.argument("bucket_name")
@click.argument("nome_segredo")
def baixar_arquivo(nome_arquivo: str, bucket_name: str, nome_segredo: str):
    config = carregar_segredo(nome_segredo)
    cliente = boto3.client(
        "s3",
        endpoint_url=config["endpoint"],
        aws_access_key_id=config["access_key"],
        aws_secret_access_key=config["secret_key"],
        region_name="auto",
    )

    caminho_destino = f"{bucket_name}/{nome_arquivo}"
    os.makedirs(os.path.dirname(caminho_destino), exist_ok=True)
    cliente.download_file(bucket_name, nome_arquivo, caminho_destino)
    print(f"Arquivo salvo em: {caminho_destino}")


if __name__ == "__main__":
    baixar_arquivo()
