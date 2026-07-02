import io   
import uuid
import duckdb
import tempfile

from pathlib import Path

# Conversão

def csv_para_parquet(conteudo: bytes) -> bytes:
    # Cria o caminho para um parquet num diretório temporário do SO
    caminho_tmp = Path(tempfile.gettempdir()) / f"{uuid.uuid4().hex}.parquet"
    try:
        # Lê o csv e escreve ele como um parquet no arquivo temporário
        duckdb.read_csv(io.BytesIO(conteudo)).write_parquet(str(caminho_tmp))
        # Lê os bytes do parquet
        return caminho_tmp.read_bytes()
    finally:
        caminho_tmp.unlink(missing_ok=True)