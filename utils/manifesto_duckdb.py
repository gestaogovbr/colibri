"""Gerenciamento de manifestos de ingestão armazenados em DuckDB."""

from pathlib import Path

import duckdb

CAMINHO_PADRAO = Path("dados/manifestos/colibri.duckdb")

TABELAS = {
    "comprasgov_pncp": "manifesto_comprasgov_pncp",
}

TABELAS_VERSOES = {
    "comprasgov_pncp": "versoes_comprasgov_pncp",
}

TABELAS_APLICACOES = {"comprasgov_pncp": "aplicacoes_comprasgov_pncp"}


def inicializar_manifesto(
    pipeline: str,
    caminho: Path = CAMINHO_PADRAO,
) -> None:
    """Cria a tabela de manifesto caso ela ainda não exista."""

    tabela = TABELAS[pipeline]
    tabela_versoes = TABELAS_VERSOES[pipeline]
    tabela_aplicacoes = TABELAS_APLICACOES[pipeline]
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
                caminho_snapshot VARCHAR,
                processado_em TIMESTAMP,
                PRIMARY KEY (tipo, ano, mes)
            )
        """)
        conexao.execute(f"""
            CREATE SEQUENCE IF NOT EXISTS seq_{tabela_aplicacoes}
            START 1
        """)

        conexao.execute(f"""
        CREATE TABLE IF NOT EXISTS {tabela_aplicacoes} (
            id_aplicacao BIGINT PRIMARY KEY
                DEFAULT nextval('seq_{tabela_aplicacoes}'),

            tipo VARCHAR NOT NULL,
            ano INTEGER NOT NULL,
            mes INTEGER NOT NULL,

            hash_anterior VARCHAR,
            hash_novo VARCHAR NOT NULL,

            caminho_deltas VARCHAR,

            qtd_insert BIGINT,
            qtd_update BIGINT,
            qtd_delete BIGINT,

            status VARCHAR NOT NULL DEFAULT 'pendente',

            criado_em TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            processado_em TIMESTAMP,

            CHECK (status IN (
                'pendente',
                'deltas_gerados',
                'processado'
            ))
        )
        """)

        conexao.execute(f"""
            ALTER TABLE {tabela}
            ADD COLUMN IF NOT EXISTS caminho_snapshot VARCHAR
        """)

        conexao.execute(f"""
            CREATE TABLE IF NOT EXISTS {tabela_versoes} (
                tipo VARCHAR NOT NULL,
                ano INTEGER NOT NULL,
                mes INTEGER NOT NULL,
                hash_sha256 VARCHAR NOT NULL,
                caminho_snapshot VARCHAR NOT NULL,
                etag VARCHAR,
                last_modified VARCHAR,
                recebido_em TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                processado_em TIMESTAMP,
                PRIMARY KEY (tipo, ano, mes, hash_sha256)
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
            SELECT etag, last_modified, hash_sha256, processado_em, caminho_snapshot
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
        "caminho_snapshot": resultado[4],
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
    caminho_snapshot: str | None = None,
) -> None:
    """Registra uma versão cuja persistência já foi confirmada."""

    tabela = TABELAS[pipeline]

    with duckdb.connect(str(caminho)) as conexao:
        conexao.execute(
            f"""
            INSERT INTO {tabela} (
                tipo, ano, mes, etag, last_modified,
                hash_sha256, caminho_snapshot, processado_em
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (tipo, ano, mes)
            DO UPDATE SET
                etag = EXCLUDED.etag,
                last_modified = EXCLUDED.last_modified,
                hash_sha256 = EXCLUDED.hash_sha256,
                caminho_snapshot = EXCLUDED.caminho_snapshot,
                processado_em = EXCLUDED.processado_em
            """,
            [
                tipo,
                ano,
                mes,
                etag,
                last_modified,
                hash_sha256,
                caminho_snapshot,
            ],
        )


def registrar_versao(
    pipeline: str,
    tipo: str,
    ano: int,
    mes: int,
    hash_sha256: str,
    caminho_snapshot: str,
    etag: str | None,
    last_modified: str | None,
    caminho: Path = CAMINHO_PADRAO,
) -> None:
    """Registrar um snapshot recebido, sem confirmar seu processamento."""

    tabela = TABELAS_VERSOES[pipeline]

    with duckdb.connect(str(caminho)) as conexao:
        conexao.execute(
            f"""
            INSERT INTO {tabela} (
                tipo, ano, mes, hash_sha256,
                caminho_snapshot, etag, last_modified
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (tipo, ano, mes, hash_sha256)
            DO NOTHING
            """,
            [
                tipo,
                ano,
                mes,
                hash_sha256,
                caminho_snapshot,
                etag,
                last_modified,
            ],
        )


def consultar_versoes(
    pipeline: str,
    tipo: str,
    ano: int,
    mes: int,
    caminho: Path = CAMINHO_PADRAO,
) -> list[dict]:
    """Consulta os snapshots conhecidos de um período."""

    tabela = TABELAS_VERSOES[pipeline]

    if not caminho.exists():
        return []

    with duckdb.connect(str(caminho), read_only=True) as conexao:
        resultados = conexao.execute(
            f"""
            SELECT
                hash_sha256,
                caminho_snapshot,
                etag,
                last_modified,
                recebido_em,
                processado_em
            FROM {tabela}
            WHERE tipo = ? AND ano = ? AND mes = ?
            ORDER BY recebido_em, hash_sha256
            """,
            [tipo, ano, mes],
        ).fetchall()

    return [
        {
            "hash_sha256": resultado[0],
            "caminho_snapshot": resultado[1],
            "etag": resultado[2],
            "last_modified": resultado[3],
            "recebido_em": resultado[4],
            "processado_em": resultado[5],
        }
        for resultado in resultados
    ]


def criar_aplicacao(
    pipeline: str,
    tipo: str,
    ano: int,
    mes: int,
    hash_anterior: str | None,
    hash_novo: str,
    caminho: Path = CAMINHO_PADRAO,
) -> int:
    """Cria uma aplicação e retorna seu identificador."""

    tabela = TABELAS_APLICACOES[pipeline]

    with duckdb.connect(str(caminho)) as conexao:
        resultado = conexao.execute(
            f"""
            INSERT INTO {tabela} (
                tipo, ano, mes,
                hash_anterior, hash_novo
            )
            VALUES (?, ?, ?, ?, ?)
            RETURNING id_aplicacao
            """,
            [
                tipo,
                ano,
                mes,
                hash_anterior,
                hash_novo,
            ],
        ).fetchone()

    return resultado[0]


def consultar_aplicacao_pendente(
    pipeline: str,
    tipo: str,
    ano: int,
    mes: int,
    caminho: Path = CAMINHO_PADRAO,
) -> dict | None:
    """Consulta a aplicação pendente, ainda não finalizada."""

    tabela = TABELAS_APLICACOES[pipeline]

    if not caminho.exists():
        return None

    with duckdb.connect(str(caminho), read_only=True) as conexao:
        resultado = conexao.execute(
            f"""
            SELECT
                id_aplicacao,
                hash_anterior,
                hash_novo,
                caminho_deltas,
                qtd_insert,
                qtd_update,
                qtd_delete,
                status,
            FROM {tabela}
            WHERE tipo = ?
                AND ano = ?
                AND mes = ?
                AND status != 'processado'
            ORDER BY  id_aplicacao
            LIMIT 1
            """,
            [tipo, ano, mes],
        ).fetchone()

    if resultado is None:
        return None

    return {
        "id_aplicacao": resultado[0],
        "hash_anterior": resultado[1],
        "hash_novo": resultado[2],
        "caminho_deltas": resultado[3],
        "qtd_insert": resultado[4],
        "qtd_update": resultado[5],
        "qtd_delete": resultado[6],
        "status": resultado[7],
    }


def registrar_deltas(
    pipeline: str,
    id_aplicacao: int,
    caminho_deltas: str,
    qtd_insert: int,
    qtd_update: int,
    qtd_delete: int,
    caminho: Path = CAMINHO_PADRAO,
) -> None:
    """Confirma a geração dos deltas de para uma aplicação."""

    tabela = TABELAS_APLICACOES[pipeline]

    with duckdb.connect(str(caminho)) as conexao:
        resultado = conexao.execute(
            f"""
            UPDATE {tabela}
            SET
                caminho_deltas = ?,
                qtd_insert = ?,
                qtd_update = ?,
                qtd_delete = ?,
                status = 'deltas_gerados'
            WHERE id_aplicacao = ?
                AND status = 'pendente'
            RETURNING id_aplicacao
            """,
            [
                caminho_deltas,
                qtd_insert,
                qtd_update,
                qtd_delete,
                id_aplicacao,
            ],
        ).fetchone()

        if resultado is None:
            raise ValueError(f"Aplicação {id_aplicacao} inexistente ou fora do estado pendente.")


def confirmar_aplicacao(pipeline: str, id_aplicacao: int, caminho: Path = CAMINHO_PADRAO) -> None:
    """Confirma a geração dos deltas para uma aplicação."""

    tabela = TABELAS[pipeline]
    tabela_versoes = TABELAS_VERSOES[pipeline]
    tabela_aplicacoes = TABELAS_APLICACOES[pipeline]

    with duckdb.connect(str(caminho)) as conexao:
        conexao.execute("BEGIN TRANSACTION")

        try:
            aplicacao = conexao.execute(
                f"""
                SELECT tipo, ano, mes, hash_anterior, hash_novo
                FROM {tabela_aplicacoes}
                WHERE id_aplicacao = ?
                  AND status = 'deltas_gerados'
            """,
                [id_aplicacao],
            ).fetchone()

            if aplicacao is None:
                raise ValueError(f"Aplicação {id_aplicacao} não pode ser confirmada.")

            tipo, ano, mes, hash_anterior, hash_novo = aplicacao

            baseline = conexao.execute(
                f"""
                SELECT hash_sha256
                FROM {tabela}
                WHERE tipo = ? AND ano = ? AND mes = ?
            """,
                [tipo, ano, mes],
            ).fetchone()

            hash_atual = baseline[0] if baseline else None

            if hash_atual != hash_anterior:
                raise ValueError("A baseline atual diverge da aplicação.")

            versao = conexao.execute(
                f"""
                SELECT caminho_snapshot, etag, last_modified
                FROM {tabela_versoes}
                WHERE tipo = ?
                  AND ano = ?
                  AND mes = ?
                  AND hash_sha256 = ?
            """,
                [tipo, ano, mes, hash_novo],
            ).fetchone()

            if versao is None:
                raise ValueError("Nova versão não encontrada.")

            caminho_snapshot, etag, last_modified = versao

            conexao.execute(
                f"""
                INSERT INTO {tabela} (
                    tipo, ano, mes, etag, last_modified,
                    hash_sha256, caminho_snapshot, processado_em
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT (tipo, ano, mes)
                DO UPDATE SET
                    etag = EXCLUDED.etag,
                    last_modified = EXCLUDED.last_modified,
                    hash_sha256 = EXCLUDED.hash_sha256,
                    caminho_snapshot = EXCLUDED.caminho_snapshot,
                    processado_em = EXCLUDED.processado_em
            """,
                [tipo, ano, mes, etag, last_modified, hash_novo, caminho_snapshot],
            )

            conexao.execute(
                f"""
                UPDATE {tabela_versoes}
                SET processado_em = COALESCE(
                    processado_em, CURRENT_TIMESTAMP
                )
                WHERE tipo = ?
                  AND ano = ?
                  AND mes = ?
                  AND hash_sha256 = ?
            """,
                [tipo, ano, mes, hash_novo],
            )

            conexao.execute(
                f"""
                UPDATE {tabela_aplicacoes}
                SET status = 'processado',
                    processado_em = CURRENT_TIMESTAMP
                WHERE id_aplicacao = ?
            """,
                [id_aplicacao],
            )

            conexao.execute("COMMIT")

        except Exception:
            conexao.execute("ROLLBACK")
            raise
