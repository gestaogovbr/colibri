import click
import utils.configurar_logging as log

from botocore.exceptions import ClientError
from utils.criar_cliente import criar_cliente

# Template provisório de log
PLACEHOLDER_ERRO_BUCKET = "Erro ao deletar objetos do bucket '%s'"


@click.command()
@click.argument("bucket_name")
@click.argument("nome_segredo")
@click.confirmation_option(prompt="Isso vai deletar todos os objetos. Confirma?")
def deletar_tudo(bucket_name: str, nome_segredo: str):
    """
    Deleta todo os arquivos do bucket especificado
    """
    cliente = criar_cliente(nome_segredo)

    # Chamada da API do S3 que lista objetos no bucket num objeto paginador
    paginator = cliente.get_paginator("list_objects_v2")

    # Utilitário do boto3 que retorna um iterador com TODAS as as páginas de resultados
    # Ex: page ->   {
    #                  "Contents": [
    #                      {"Key": "logs/arquivo1.json", "Size": 1024, "LastModified": ..., "ETag": ...},
    #                      {"Key": "logs/arquivo2.json", "Size": 2048, "LastModified": ..., "ETag": ...},
    #                  ],
    #                  "KeyCount": 2,
    #                  "IsTruncated": True,
    #                  "Name": "meu-bucket",
    #               }
    pages = paginator.paginate(Bucket=bucket_name)

    # Cada página tem no máximo 1000 chaves, o mesmo limite do delete_objects
    total = 0
    try:
        for page in pages:
            objetos = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
            if objetos:
                cliente.delete_objects(Bucket=bucket_name, Delete={"Objects": objetos})
                total += len(objetos)
    except ClientError as e:
        # Identificar qual erro ocorreria, como bucket vazio ou problemas de permissão
        # if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
        raise Exception(
            PLACEHOLDER_ERRO_BUCKET % (bucket_name)
        ) from e

    print(f"{total} objeto(s) deletado(s) de '{bucket_name}'.")


if __name__ == "__main__":
    deletar_tudo()
