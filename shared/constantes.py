import os


CATALOGO_LOCAL = "meta.ducklake"
_SESSION_DB = "ducklake_session.duckdb"
_RAIZ_PROJETO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CAMINHO_SEGREDOS = os.path.join(_RAIZ_PROJETO, ".segredos.yml")
SEGREDO_PADRAO = "colibri-token-desenvolvedor"