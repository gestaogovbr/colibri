import boto3
import click

from shared.carregar_segredo import carregar_segredo


@click.command()
@click.argument("bucket_name")
@click.argument("nome_segredo", default="colibri-token-desenvolvedor")
def listar_buckets(bucket_name: str, nome_segredo: str):
    config = carregar_segredo(nome_segredo)
    cliente = boto3.client(
        "s3",
        endpoint_url=config["endpoint"],
        aws_access_key_id=config["access_key"],
        aws_secret_access_key=config["secret_key"],
        region_name="auto",
    )
    for obj in cliente.list_objects_v2(Bucket=bucket_name).get("Contents", []):
        print(obj["LastModified"], f"{obj['Size']:>10} bytes", obj["Key"])


if __name__ == "__main__":
    listar_buckets()
