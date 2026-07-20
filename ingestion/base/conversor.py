import io
import uuid
import duckdb
import tempfile

from pathlib import Path


class Conversor:
    """
    Classe responsável por converter arquivos CSV para Parquet
    """
    def __init__(self):
        pass


    @staticmethod
    def csv_para_parquet(conteudo: bytes) -> bytes:
        """Converte o conteúdo CSV em bytes para Parquet em bytes"""
        caminho_tmp = Path(tempfile.gettempdir()) / f"{uuid.uuid4().hex}.parquet"
        try:
            duckdb.read_csv(io.BytesIO(conteudo)).write_parquet(str(caminho_tmp))
            return caminho_tmp.read_bytes()
        finally:
            caminho_tmp.unlink(missing_ok=True)


    @staticmethod
    def json_para_parquet(conteudo: bytes) -> bytes:
        """Converte o conteúdo JSON em bytes para Parquet em bytes"""
        caminho_tmp = Path(tempfile.gettempdir()) / f"{uuid.uuid4().hex}.parquet"
        try:
            duckdb.read_json(io.BytesIO(conteudo)).write_parquet(str(caminho_tmp))
            return caminho_tmp.read_bytes()
        finally:
            caminho_tmp.unlink(missing_ok=True)
