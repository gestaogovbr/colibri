"""Controle de concorrência da ingestão PNCP/DELOG."""

from pathlib import Path

from filelock import FileLock


def bloquear_destino(destino: Path) -> FileLock:
    """Retorna um lock exclusivo associado ao arquivo de destino."""

    destino.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    arquivo_lock = destino.with_name(f".{destino.name}.lock")

    return FileLock(
        str(arquivo_lock),
        timeout=300,
    )
