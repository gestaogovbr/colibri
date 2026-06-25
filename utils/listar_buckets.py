import click

from utils.carregar_segredo import carregar_segredo
from utils.criar_cliente import criar_cliente


@click.command()
@click.argument("bucket_name")
@click.argument("nome_segredo", default="colibri-token-desenvolvedor")
def listar_buckets(bucket_name: str, nome_segredo: str):
    config = carregar_segredo(nome_segredo)
    cliente = criar_cliente(config)
    for obj in cliente.list_objects_v2(Bucket=bucket_name).get("Contents", []):
        print(obj["LastModified"], f"{obj['Size']:>10} bytes", obj["Key"])


if __name__ == "__main__":
    listar_buckets()
