import os
import click

from utils.carregar_segredo import carregar_segredo
from utils.criar_cliente import criar_cliente


@click.command()
@click.argument("nome_arquivo")
@click.argument("bucket_name")
@click.argument("nome_segredo")
def baixar_arquivo(nome_arquivo: str, bucket_name: str, nome_segredo: str):
    """
    Baixa um arquivo de um bucket S3 usando credenciais armazenadas em um segredo
    """
    config = carregar_segredo(nome_segredo)
    cliente = criar_cliente(config)

    # Caminho de destino para salvar o arquivo baixado do bucket
    caminho_destino = f"{bucket_name}/{nome_arquivo}"
    os.makedirs(os.path.dirname(caminho_destino), exist_ok=True)

    cliente.download_file(bucket_name, nome_arquivo, caminho_destino)
    print(f"Arquivo salvo em: {caminho_destino}")


if __name__ == "__main__":
    baixar_arquivo()
