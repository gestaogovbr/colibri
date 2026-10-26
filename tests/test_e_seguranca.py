"""Grupo E — segurança: sem credenciais reais, sem prod, sem DuckLake, sem tocar em dados/ real, sem vazar segredos."""

import logging
import re
import socket
from pathlib import Path

import boto3
import pytest
from apoio_delog import CSV_PADRAO, FAKE_CONFIG, FakeSessao, caminho_saida, criar_parquet_local, executar
from botocore.exceptions import ClientError

RAIZ = Path(__file__).resolve().parents[1]
SEGREDOS_FAKE = [FAKE_CONFIG["access_key"], FAKE_CONFIG["secret_key"]]


# 1. credenciais
def test_segredo_usado_e_o_falso(r2, carregar_segredo_mock):
    assert r2.carregar_segredo is carregar_segredo_mock
    assert r2.carregar_segredo()["access_key"].startswith("FAKE")
    assert r2.carregar_segredo()["secret_key"].startswith("FAKE")


def test_criar_cliente_real_recebe_apenas_credenciais_falsas(monkeypatch):
    import importlib
    from unittest.mock import MagicMock

    mod = importlib.import_module("utils.criar_cliente")
    fake_client = MagicMock()
    monkeypatch.setattr(boto3, "client", fake_client)
    mod.criar_cliente(FAKE_CONFIG)
    kw = fake_client.call_args.kwargs
    assert kw["aws_access_key_id"].startswith("FAKE") and kw["aws_secret_access_key"].startswith("FAKE")
    assert kw["endpoint_url"].startswith("https://fake-")
    assert kw["region_name"] == "auto"


def test_carregar_segredo_le_apenas_arquivo_informado(tmp_path):
    import importlib

    mod = importlib.import_module("utils.carregar_segredo")
    arq = tmp_path / "segredos_fake.yml"
    arq.write_text("colibri-token-desenvolvedor:\n  endpoint: e\n  access_key: FAKE_A\n  secret_key: FAKE_S\n")
    assert mod.carregar_segredo("colibri-token-desenvolvedor", str(arq))["access_key"] == "FAKE_A"
    with pytest.raises(KeyError):
        mod.carregar_segredo("nao-existe", str(arq))
    with pytest.raises(FileNotFoundError):
        mod.carregar_segredo("x", str(tmp_path / "naoexiste.yml"))


# 2. nunca prod
def test_constantes_dev_e_prod_distintas():
    from utils.constantes import BUCKET_DESENVOLVIMENTO, BUCKET_PRODUCAO

    assert BUCKET_DESENVOLVIMENTO == "colibri-dev" and BUCKET_PRODUCAO == "colibri-prod"


def test_modulos_nao_referenciam_bucket_de_producao():
    for nome in ("extract_delog.py", "extract_delog_r2.py"):
        src = (RAIZ / "ingestion/pncp_comprasgov" / nome).read_text()
        assert "BUCKET_PRODUCAO" not in src and "colibri-prod" not in src.replace("--bucket colibri-prod", "")


def test_prod_bloqueado_na_cli_e_na_funcao(r2, rodar_main, cliente_s3):
    criar_parquet_local()
    with pytest.raises(ValueError):
        r2.enviar_para_r2("compras", 2025, 8, "colibri-prod")
    with pytest.raises(SystemExit):
        rodar_main(
            r2, "--ano", "2025", "--mes", "8", "--upload", "--bucket", "colibri-prod", sessao=FakeSessao.status(404)
        )
    assert cliente_s3.mock_calls == []  # o auditor autouse também confere isso


# 3. DuckLake
def test_modulos_nao_tocam_ducklake_nem_dbt():
    for nome in ("extract_delog.py", "extract_delog_r2.py"):
        src = (RAIZ / "ingestion/pncp_comprasgov" / nome).read_text().lower()
        # docstring afirma "não escreve no DuckLake": exclui só a menção textual
        codigo = re.sub(r'""".*?"""', "", src, flags=re.S)
        for proibido in ("attach", "ducklake", "subprocess", "dbt"):
            assert proibido not in codigo, f"{nome} referencia {proibido}"


def test_execucao_nao_cria_catalogo_ducklake(modulo, raiz_exec):
    executar(modulo, FakeSessao.com_csv(CSV_PADRAO))
    assert not list(raiz_exec.rglob("*.ducklake")) and not list(raiz_exec.rglob("ducklake_session*"))


# 4. dados/ real
def test_cwd_isolado_dados_real_intocado(modulo, raiz_exec):
    assert not str(raiz_exec).startswith(str(RAIZ))
    executar(modulo, FakeSessao.com_csv(CSV_PADRAO))
    assert caminho_saida().resolve().is_relative_to(raiz_exec.resolve())
    assert not caminho_saida().resolve().is_relative_to((RAIZ / "dados").resolve())


# 5. logs sem credenciais
def _nao_vaza(texto):
    for s in SEGREDOS_FAKE:
        assert s not in texto


def test_logs_do_fluxo_de_upload_nao_vazam_segredos(r2, cliente_s3, caplog):
    caplog.set_level(logging.DEBUG)
    criar_parquet_local()
    cliente_s3.head_object.side_effect = [ClientError({"Error": {"Code": "404", "Message": "m"}}, "HeadObject"), {}]
    r2.enviar_para_r2("compras", 2025, 8, "colibri-dev")
    _nao_vaza(caplog.text)


@pytest.mark.parametrize("codigo", ["403", "InvalidAccessKeyId"])
def test_logs_de_falha_de_autenticacao_nao_vazam_segredos(r2, cliente_s3, caplog, codigo):
    caplog.set_level(logging.DEBUG)
    criar_parquet_local()
    cliente_s3.head_object.side_effect = ClientError({"Error": {"Code": codigo, "Message": "denied"}}, "HeadObject")
    assert r2.enviar_para_r2("compras", 2025, 8, "colibri-dev") == "erro"
    _nao_vaza(caplog.text)


def test_logs_do_main_completo_nao_vazam_segredos(r2, rodar_main, caplog, capsys):
    caplog.set_level(logging.DEBUG)
    rodar_main(
        r2, "--ano", "2025", "--mes", "8", "--upload", "--bucket", "colibri-dev", sessao=FakeSessao.com_csv(CSV_PADRAO)
    )
    saida = capsys.readouterr()
    _nao_vaza(caplog.text + saida.out + saida.err)


# 6. mocks / tmp
def test_guardas_de_rede_e_boto3_funcionam():
    with pytest.raises(RuntimeError):
        boto3.client("s3")
    with pytest.raises(RuntimeError):
        socket.create_connection(("repositorio.dados.gov.br", 443))


def test_tempdir_redirecionado(tmp_sistema, tmp_path):
    assert Path(tmp_sistema).is_relative_to(tmp_path)


def test_modulo_extract_delog_nao_oferece_upload(local, rodar_main):
    """extract_delog.py é 'local e isolado': não aceita --upload/--bucket nem importa boto3/R2."""
    with pytest.raises(SystemExit) as e:
        rodar_main(local, "--ano", "2025", "--mes", "8", "--upload", sessao=FakeSessao.status(404))
    assert e.value.code == 2
    assert not hasattr(local, "enviar_para_r2") and not hasattr(local, "boto3")
