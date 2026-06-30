import os
from pathlib import Path


CATALOGO_LOCAL = "meta.ducklake"
BUCKET = "colibri-dev"
SESSION_DB = "ducklake_session.duckdb"
NOME_SEGREDO = "colibri-token-desenvolvedor"
RAIZ_PROJETO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CAMINHO_SEGREDOS = os.path.join(RAIZ_PROJETO, ".segredos.yml")
CAMINHO_META = f"s3://{BUCKET}/meta.ducklake"
DATA_PATH = f"s3://{BUCKET}/lake/"
DBT_DIR = Path(RAIZ_PROJETO) / "dbt"