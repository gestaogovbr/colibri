"""Grupo A — parâmetros, URLs, caminhos locais e chaves do R2."""

import pytest

from tests.comprasgov.apoio import (
    PREFIXOS,
    URL_BASE,
    FakeSessao,
    caminho_saida,
    chave_r2,
    criar_parquet_local,
    executar,
)

PERIODOS = [(2025, 8), (2025, 6), (2021, 12), (2024, 1), (2026, 10)]


@pytest.mark.parametrize("tipo,prefixo", PREFIXOS.items())
@pytest.mark.parametrize("ano,mes", PERIODOS)
def test_url_por_tipo_ano_mes(modulo, tipo, prefixo, ano, mes):
    sessao = FakeSessao.status(404)
    assert executar(modulo, sessao, tipo, ano, mes) == "indisponivel"
    assert sessao.urls == [f"{URL_BASE}/{ano}/{mes:02d}/{prefixo}_{ano}_{mes:02d}.csv.zip"]


def test_mes_com_dois_digitos_na_url(modulo):
    sessao = FakeSessao.status(404)
    executar(modulo, sessao, "compras", 2025, 6)
    assert sessao.urls[0].endswith("/2025/06/compra_2025_06.csv.zip")


def test_tres_tipos_conhecidos(modulo):
    assert modulo.TIPOS == PREFIXOS


def test_modulos_concordam_em_url_base_e_tipos(local, r2):
    assert local.URL_BASE == r2.URL_BASE == URL_BASE
    assert local.TIPOS == r2.TIPOS


def test_tipo_invalido_levanta_keyerror(modulo):
    with pytest.raises(KeyError):
        executar(modulo, FakeSessao.status(404), "inexistente", 2025, 8)


# ---- CLI (regras existentes) ----


@pytest.mark.parametrize("mes", ["0", "13", "-1", "abc"])
def test_cli_rejeita_mes_invalido(modulo, rodar_main, mes):
    with pytest.raises(SystemExit) as e:
        rodar_main(modulo, "--ano", "2025", "--mes", mes, sessao=FakeSessao.status(404))
    assert e.value.code == 2


def test_cli_rejeita_tipo_invalido(modulo, rodar_main):
    with pytest.raises(SystemExit) as e:
        rodar_main(modulo, "--ano", "2025", "--mes", "8", "--tipo", "xyz", sessao=FakeSessao.status(404))
    assert e.value.code == 2


def test_cli_exige_ano_e_mes(modulo, rodar_main):
    with pytest.raises(SystemExit) as e:
        rodar_main(modulo, "--mes", "8")
    assert e.value.code == 2


def test_cli_extract_rejeita_ano_anterior_a_2021(local, rodar_main):
    with pytest.raises(SystemExit) as e:
        rodar_main(local, "--ano", "2019", "--mes", "8", sessao=FakeSessao.status(404))
    assert e.value.code == 2


@pytest.mark.limitacao
def test_LIMITACAO_cli_r2_deveria_rejeitar_ano_anterior_a_2021(r2, rodar_main):
    """extract_delog valida ano >= 2021; extract_delog_r2 não (regras inconsistentes)."""
    with pytest.raises(SystemExit) as e:
        rodar_main(r2, "--ano", "1999", "--mes", "8", sessao=FakeSessao.status(404))
    assert e.value.code == 2


@pytest.mark.limitacao
@pytest.mark.parametrize("mes", [0, 13])
def test_LIMITACAO_processar_tipo_nao_valida_mes(modulo, mes):
    """Chamado como função (fora da CLI), mes inválido gera URL inexistente em vez de ValueError."""
    with pytest.raises(ValueError):
        executar(modulo, FakeSessao.status(404), "compras", 2025, mes)


# ---- caminhos locais e chaves R2 ----


def test_caminho_destino_compras_2025_06(r2):
    assert r2.caminho_destino("compras", 2025, 6) == caminho_saida("compras", 2025, 6)
    assert r2.caminho_destino("compras", 2025, 6).as_posix() == (
        "dados/pncp_comprasgov_delog/compras/2025/06/compras_2025_06.parquet"
    )


@pytest.mark.parametrize("tipo", PREFIXOS)
def test_arquivo_criado_no_caminho_esperado(modulo, tipo):
    from tests.comprasgov.apoio import CSV_PADRAO

    assert executar(modulo, FakeSessao.com_csv(CSV_PADRAO), tipo, 2025, 6) == "criado"
    assert caminho_saida(tipo, 2025, 6).is_file()


@pytest.mark.parametrize("tipo", PREFIXOS)
@pytest.mark.parametrize("ano,mes", [(2025, 6), (2025, 12), (2021, 1)])
def test_chave_r2_segue_padrao(r2, cliente_s3, tipo, ano, mes):
    from botocore.exceptions import ClientError

    criar_parquet_local(tipo, ano, mes)
    nao_achou = ClientError({"Error": {"Code": "404", "Message": "x"}}, "HeadObject")
    cliente_s3.head_object.side_effect = [nao_achou, {}]
    assert r2.enviar_para_r2(tipo, ano, mes, "colibri-dev") == "enviado"
    assert cliente_s3.head_object.call_args_list[0].kwargs == {
        "Bucket": "colibri-dev",
        "Key": chave_r2(tipo, ano, mes),
    }
    assert chave_r2("compras", 2025, 6) == "pncp_comprasgov_delog/compras/2025/06/compras_2025_06.parquet"
    