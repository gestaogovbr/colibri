import os
import click

from utils.carregar_segredo import carregar_segredo
from utils.criar_cliente import criar_cliente


def baixar_arquivo_do_bucket(nome_arquivo: str, bucket_name: str, nome_segredo: str, caminho_destino: str) -> None:
    config = carregar_segredo(nome_segredo)
    cliente = criar_cliente(config)
    os.makedirs(os.path.dirname(caminho_destino), exist_ok=True)

    cliente.download_file(bucket_name, nome_arquivo, caminho_destino)


@click.command()
@click.argument("nome_arquivo")
@click.argument("bucket_name")
@click.argument("nome_segredo")
@click.argument("caminho_destino")
def baixar_arquivo(nome_arquivo: str, bucket_name: str, nome_segredo: str, caminho_destino: str):
    baixar_arquivo_do_bucket(nome_arquivo, bucket_name, nome_segredo, caminho_destino)
    print(f"Arquivo salvo em: {caminho_destino}")


if __name__ == "__main__":
    baixar_arquivo()