import os


_RAIZ_PROJETO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOGO_LOCAL = os.path.join(_RAIZ_PROJETO, "meta.ducklake")
_SESSION_DB = os.path.join(_RAIZ_PROJETO, "ducklake_session.duckdb")
CAMINHO_SEGREDOS = os.path.join(_RAIZ_PROJETO, ".segredos.yml")
SEGREDO_PADRAO = "colibri-token-desenvolvedor"