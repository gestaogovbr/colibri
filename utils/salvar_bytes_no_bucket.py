import io

from utils.carregar_segredo import carregar_segredo
from utils.criar_cliente import criar_cliente


def salvar_bytes_no_bucket(
    conteudo: bytes, bucket_name: str, nome_segredo: str, nome_no_bucket: str
) -> None:
    config = carregar_segredo(nome_segredo)
    cliente = criar_cliente(config)
    cliente.upload_fileobj(io.BytesIO(conteudo), bucket_name, nome_no_bucket)
