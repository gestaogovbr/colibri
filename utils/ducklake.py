"""
Gerencia conexões DuckDB com um lake no formato DuckLake, cujo catálogo ocorre localmente, mas
sincronizado com um bucket S3/R2, enquanto os dados (parquets) ocorrem direto no object storage do bucket.

Uso típico:
    `conectar()` baixa o catálogo remoto, anexa o lake e retorna uma conexão pronta para uso;
    `fechar()` desfaz isso, subindo de volta o catálogo atualizado para o bucket.
"""

import os
import duckdb

from urllib.parse import urlparse
from utils.carregar_segredo import carregar_segredo
from utils.baixar_catalogo import baixar_catalogo
from utils.criar_cliente import criar_cliente
from utils.constantes import CATALOGO_LOCAL, SESSION_DB


def _parsear_s3(caminho_meta: str) -> tuple[str, str]:
    """
    Ex:
        >>> _parsear_s3("s3://meu-bucket/meta.ducklake")
        output: ('meu-bucket', 'meta.ducklake')
    """
    # Retorna um obj ParseResult (tupla de 6 campos): (scheme, netloc, path, params, query, fragment)
    parsed = urlparse(caminho_meta)

    # Puxa só o campo de netloc e path sem "/" no começo
    # Aqui, netloc = nome do bucket pós-parsing
    return parsed.netloc, parsed.path.lstrip("/")


def _subir_catalogo(cliente, bucket: str, chave: str):
    """
    Upload do meta.ducklake local no bucket, usando método do boto3
    """
    cliente.upload_file(CATALOGO_LOCAL, bucket, chave)
    print(f"[ducklake] Catálogo sincronizado para s3://{bucket}/{chave}")


def _checkpoint_catalogo():
    """
    Realiza um checkpoint do catálogo (força a gravação das alterações do WAL no arquivo meta.ducklake)
    """
    try:
        # Cria lock no meta.ducklake e cria um objeto connection para forçar manipular o banco
        con = duckdb.connect(CATALOGO_LOCAL)

        # Força o duckdb a gravar as alterações do WAL no meta.ducklake
        con.execute("FORCE CHECKPOINT")

        # Libera lock do meta.ducklake
        con.close()
    except Exception as e:
        print(f"[ducklake] Aviso: checkpoint falhou: {e}")


def _nova_conexao(config: dict) -> duckdb.DuckDBPyConnection:
    """
    Cria uma conexão duckdb local, contendo todas as extensões e credenciais pra falar com o bucket
    """
    endpoint = urlparse(config["endpoint"]).netloc
    if os.path.exists(SESSION_DB):
        os.remove(SESSION_DB)
    con = duckdb.connect(SESSION_DB)
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    con.execute("INSTALL ducklake")
    con.execute("LOAD ducklake")
    con.execute(f"SET GLOBAL s3_endpoint = '{endpoint}'")
    con.execute(f"SET GLOBAL s3_access_key_id = '{config['access_key']}'")
    con.execute(f"SET GLOBAL s3_secret_access_key = '{config['secret_key']}'")
    con.execute("SET GLOBAL s3_region = 'auto'")
    con.execute("SET GLOBAL s3_url_style = 'path'")
    return con


def conectar(
    caminho_meta: str, data_path: str, nome_segredo: str
) -> duckdb.DuckDBPyConnection:
    """
    Baixa o catálogo remoto e retorna uma conexão duckdb com o lake já anexado, pronta para uso
    """
    config = carregar_segredo(nome_segredo)
    cliente = criar_cliente(config)

    # Obtém o nome do bucket e a chave, a partir da URL do objeto do catálogo no bucket
    bucket, chave = _parsear_s3(caminho_meta)
    baixar_catalogo(cliente, bucket, chave)

    # Cria uma conexão com o SESSION_DB ("ducklake_session.duckdb")
    con = _nova_conexao(config)

    # Conecta o catálogo baixado do bucket com o caminho dos parquets no bucket
    con.execute(
        f"ATTACH 'ducklake:{CATALOGO_LOCAL}' AS lake (DATA_PATH '{data_path}', OVERRIDE_DATA_PATH TRUE)"
    )
    return con


def fechar(con: duckdb.DuckDBPyConnection, caminho_meta: str, nome_segredo: str):
    """
    Desanexa o lake, libera a sessão local e sincroniza o catálogo atualizado de volta pro bucket
    """
    con.execute("DETACH lake")
    con.close()

    # Remove o banco de sessão local, já que não é mais necessário
    if os.path.exists(SESSION_DB):
        os.remove(SESSION_DB)

    # Garante que as alterações do WAL foram gravadas no meta.ducklake antes do upload
    _checkpoint_catalogo()

    config = carregar_segredo(nome_segredo)
    cliente = criar_cliente(config)

    bucket, chave = _parsear_s3(caminho_meta)
    _subir_catalogo(cliente, bucket, chave)
