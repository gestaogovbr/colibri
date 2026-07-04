import boto3


def criar_cliente(config: dict):
    """
    Cria e retorna um cliente S3 usando as credenciais fornecidas no dicionário de config
    """
    return boto3.client(
        "s3",
        endpoint_url=config["endpoint"],
        aws_access_key_id=config["access_key"],
        aws_secret_access_key=config["secret_key"],
        region_name="auto",
    )
