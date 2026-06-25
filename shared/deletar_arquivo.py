import click
import shared.configurar_logging as log

from botocore.exceptions import ClientError
from shared.criar_cliente import criar_cliente


@click.command()
@click.argument("nome_arquivo")
@click.argument("bucket_name")
@click.argument("nome_segredo")
def deletar_arquivo(nome_arquivo: str, bucket_name: str, nome_segredo: str):
    """
    Deleta um arquivo específico do bucket especificado
    """
    cliente = criar_cliente(nome_segredo)

    try:
        cliente.head_object(Bucket=bucket_name, Key=nome_arquivo)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            raise Exception(
                log.ARQUIVO_NAO_ENCONTRADO_NO_BUCKET % (nome_arquivo, bucket_name)
            ) from e
        raise

    cliente.delete_object(Bucket=bucket_name, Key=nome_arquivo)
    print(f"Deletado: {nome_arquivo}")


if __name__ == "__main__":
    deletar_arquivo()
