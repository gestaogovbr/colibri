"""Smoke tests: garantem que o código é importável e que a CLI carrega.

Não exercem lógica de negócio (que depende de credenciais R2 e da rede); apenas
verificam que todos os módulos importam sem erro e que a CLI responde a `--help`.
"""

import importlib
from pathlib import Path

import pytest
from click.testing import CliRunner

RAIZ = Path(__file__).resolve().parent.parent
PACOTES = ["utils", "ingestion"]


def _descobrir_modulos() -> list[str]:
    modulos: list[str] = []
    for pacote in PACOTES:
        for arquivo in sorted((RAIZ / pacote).rglob("*.py")):
            if "__pycache__" in arquivo.parts:
                continue
            partes = arquivo.relative_to(RAIZ).with_suffix("").parts
            if partes[-1] == "__init__":
                partes = partes[:-1]
            modulos.append(".".join(partes))
    return modulos


@pytest.mark.parametrize("modulo", _descobrir_modulos())
def test_modulo_importa(modulo: str) -> None:
    importlib.import_module(modulo)


def test_cli_help() -> None:
    from cli import cli

    resultado = CliRunner().invoke(cli, ["--help"])
    assert resultado.exit_code == 0, resultado.output
