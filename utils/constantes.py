import os
from pathlib import Path


BUCKET = "colibri-dev"
NOME_SEGREDO = "colibri-token-desenvolvedor"
RAIZ_PROJETO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOGO_LOCAL = os.path.join(RAIZ_PROJETO, "meta.ducklake")
SESSION_DB = os.path.join(RAIZ_PROJETO, "ducklake_session.duckdb")
CAMINHO_SEGREDOS = os.path.join(RAIZ_PROJETO, ".segredos.yml")
CAMINHO_META = f"s3://{BUCKET}/meta.ducklake"
DATA_PATH = f"s3://{BUCKET}/lake/"
DBT_DIR = Path(RAIZ_PROJETO) / "dbt"