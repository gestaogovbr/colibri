import os
from pathlib import Path

CATALOGO_LOCAL = "meta.ducklake"
SESSION_DB = "ducklake_session.duckdb"
BUCKET_DESENVOLVIMENTO = "colibri-dev"
BUCKET_PRODUCAO = "colibri-prod"
BUCKET_ARQUIVOS = "colibri-arquivos"
NOME_SEGREDO_DESENVOLVEDOR = "colibri-token-desenvolvedor"
NOME_SEGREDO_VISUALIZADOR = "colibri-token-visualizador"
RAIZ_PROJETO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CAMINHO_SEGREDOS = os.path.join(RAIZ_PROJETO, ".segredos.yml")
CAMINHO_META = f"s3://{BUCKET_PRODUCAO}/meta.ducklake"
DATA_PATH = f"s3://{BUCKET_PRODUCAO}/lake/"
DBT_DIR = Path(RAIZ_PROJETO) / "dbt"
