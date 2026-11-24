"""Testes das funções de formatação usadas na saída da CLI."""

import pytest

import cli


@pytest.mark.parametrize(
    ("bytes_", "esperado"),
    [
        (999, "999.0 B"),
        (1000, "1.0 KB"),
        (1_500_000, "1.5 MB"),
        (26_490_000_000, "26.5 GB"),  # o caso do painel do Cloudflare (issue #128)
        (1_500_000_000_000, "1.5 TB"),
    ],
)
def test_tamanho_em_unidades_decimais(bytes_, esperado):
    assert cli._tamanho(bytes_) == esperado
