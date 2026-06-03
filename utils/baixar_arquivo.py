import os
import click

from utils.carregar_segredo import carregar_segredo
from utils.criar_cliente import criar_cliente


<<<<<<< HEAD:utils/baixar_arquivo.py
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
=======
def baixar_arquivo_do_bucket(nome_arquivo: str, bucket_name: str, nome_segredo: str, caminho_destino: str) -> None:
    config = carregar_segredo(nome_segredo)
    cliente = boto3.client(
        "s3",
        endpoint_url=config["endpoint"],
        aws_access_key_id=config["access_key"],
        aws_secret_access_key=config["secret_key"],
        region_name="auto",
    )
>>>>>>> ab195e0 (Corrige lógica de cálculo de metadados e sobe manifesto no bucket):shared/baixar_arquivo.py
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
