"""Gerenciamento de manifestos de ingestão armazenados em DuckDB."""

from pathlib import Path

import duckdb

CAMINHO_PADRAO = Path("dados/manifestos/colibri.duckdb")

TABELAS = {
    "comprasgov_pncp": "manifesto_comprasgov_pncp",
}


def inicializar_manifesto(
    pipeline: str,
    caminho: Path = CAMINHO_PADRAO,
) -> None:
    """Cria a tabela de manifesto caso ela ainda não exista."""

    tabela = TABELAS[pipeline]
    caminho.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(caminho)) as conexao:
        conexao.execute(f"""
            CREATE TABLE IF NOT EXISTS {tabela} (
                tipo VARCHAR NOT NULL,
                ano INTEGER NOT NULL,
                mes INTEGER NOT NULL,
                etag VARCHAR,
                last_modified VARCHAR,
                hash_sha256 VARCHAR,
                processado_em TIMESTAMP,
                PRIMARY KEY (tipo, ano, mes)
            )
        """)


def consultar_entrada(
    pipeline: str,
    tipo: str,
    ano: int,
    mes: int,
    caminho: Path = CAMINHO_PADRAO,
) -> dict | None:
    """Consulta a última versão processada de um arquivo."""

    tabela = TABELAS[pipeline]

    if not caminho.exists():
        return None

    with duckdb.connect(str(caminho), read_only=True) as conexao:
        resultado = conexao.execute(
            f"""
            SELECT etag, last_modified, hash_sha256, processado_em
            FROM {tabela}
            WHERE tipo = ? AND ano = ? AND mes = ?
            """,
            [tipo, ano, mes],
        ).fetchone()

    if resultado is None:
        return None

    return {
        "etag": resultado[0],
        "last_modified": resultado[1],
        "hash_sha256": resultado[2],
        "processado_em": resultado[3],
    }


def registrar_entrada(
    pipeline: str,
    tipo: str,
    ano: int,
    mes: int,
    etag: str | None,
    last_modified: str | None,
    hash_sha256: str | None,
    caminho: Path = CAMINHO_PADRAO,
) -> None:
    """Registra uma versão cuja persistência já foi confirmada."""

    tabela = TABELAS[pipeline]

    with duckdb.connect(str(caminho)) as conexao:
        conexao.execute(
            f"""
            INSERT INTO {tabela} (
                tipo, ano, mes, etag, last_modified,
                hash_sha256, processado_em
            )
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (tipo, ano, mes)
            DO UPDATE SET
                etag = EXCLUDED.etag,
                last_modified = EXCLUDED.last_modified,
                hash_sha256 = EXCLUDED.hash_sha256,
                processado_em = EXCLUDED.processado_em
            """,
            [
                tipo,
                ano,
                mes,
                etag,
                last_modified,
                hash_sha256,
            ],
        )
