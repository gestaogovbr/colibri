"""Fixtures globais. Garantias aplicadas a TODOS os testes (autouse):

- cwd isolado em tmp_path (o `dados/` relativo da pipeline nunca é o real);
- tempdir do sistema redirecionado para tmp_path;
- rede bloqueada (socket) e boto3.client/resource reais bloqueados;
- segredos e cliente S3 substituídos por falsos/mocks;
- auditor: falha se o cliente mock receber qualquer chamada envolvendo `colibri-prod`;
- guarda de sessão: `dados/`, `meta.ducklake` e `.segredos.yml` reais não podem mudar.

Testes que dependem de serviços reais usam @pytest.mark.servico_real e só rodam com
COLIBRI_TESTES_SERVICO_REAL=1.
"""

import importlib
import os
import socket
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import boto3
import pytest

RAIZ = Path(__file__).resolve().parents[1]
for _p in (RAIZ, Path(__file__).parent):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from tests.comprasgov.apoio import FAKE_CONFIG  # noqa: E402

MODULOS = ["ingestion.comprasgov.extract", "ingestion.comprasgov.extract_r2"]


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "limitacao: teste que expõe limitação do código atual (falha até o código mudar)"
    )
    config.addinivalue_line("markers", "integracao: integração local (sem serviços reais)")
    config.addinivalue_line("markers", "servico_real: acessa serviço real; desativado por padrão")


def pytest_collection_modifyitems(config, items):
    if os.environ.get("COLIBRI_TESTES_SERVICO_REAL") == "1":
        return
    skip = pytest.mark.skip(reason="serviço real: defina COLIBRI_TESTES_SERVICO_REAL=1 para autorizar")
    for item in items:
        if "servico_real" in item.keywords:
            item.add_marker(skip)


# ---------- guarda de sessão: nada real pode mudar ----------


def _snapshot_real():
    alvos = [RAIZ / "dados", RAIZ / "meta.ducklake", RAIZ / "ducklake_session.duckdb", RAIZ / ".segredos.yml"]
    snap = {}
    for alvo in alvos:
        if alvo.is_dir():
            for f in alvo.rglob("*"):
                st = f.stat()
                snap[str(f)] = (st.st_size, st.st_mtime_ns)
        elif alvo.exists():
            st = alvo.stat()
            snap[str(alvo)] = (st.st_size, st.st_mtime_ns)
    return snap


@pytest.fixture(scope="session", autouse=True)
def guarda_dados_reais():
    antes = _snapshot_real()
    yield
    assert _snapshot_real() == antes, "Algum teste alterou dados/, meta.ducklake ou .segredos.yml REAIS"


# ---------- isolamento por teste ----------


@pytest.fixture(autouse=True)
def isolamento(request, tmp_path, monkeypatch):
    cwd = tmp_path / "cwd"
    tmp_sistema = tmp_path / "tmp_sistema"
    cwd.mkdir()
    tmp_sistema.mkdir()
    monkeypatch.chdir(cwd)
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_sistema))
    if request.node.get_closest_marker("servico_real"):
        yield
        return

    def _bloqueado(*args, **kwargs):
        raise RuntimeError("Acesso real à rede/boto3 bloqueado nos testes")

    monkeypatch.setattr(socket.socket, "connect", _bloqueado)
    monkeypatch.setattr(socket, "create_connection", _bloqueado)
    monkeypatch.setattr(boto3, "client", _bloqueado)
    monkeypatch.setattr(boto3, "resource", _bloqueado)
    for var in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
        monkeypatch.setenv(var, "FAKE_" + var)
    yield


@pytest.fixture
def raiz_exec():
    """Diretório de trabalho isolado (== cwd durante o teste)."""
    return Path.cwd()


@pytest.fixture
def tmp_sistema():
    return Path(tempfile.gettempdir())


@pytest.fixture
def cliente_s3():
    cliente = MagicMock(name="cliente_s3")
    cliente.head_object.return_value = {}
    return cliente


@pytest.fixture
def carregar_segredo_mock():
    return MagicMock(name="carregar_segredo", return_value=dict(FAKE_CONFIG))


@pytest.fixture
def criar_cliente_mock(cliente_s3):
    return MagicMock(name="criar_cliente", return_value=cliente_s3)


@pytest.fixture(autouse=True)
def substituir_segredos_e_cliente(monkeypatch, carregar_segredo_mock, criar_cliente_mock, cliente_s3):
    r2 = importlib.import_module("ingestion.comprasgov.extract_r2")
    salvar = importlib.import_module("utils.salvar_arquivo_no_bucket")
    for mod in (r2, salvar):
        monkeypatch.setattr(mod, "carregar_segredo", carregar_segredo_mock)
        monkeypatch.setattr(mod, "criar_cliente", criar_cliente_mock)
    yield
    # Auditor: nada pode ter tocado o bucket de produção.
    assert "colibri-prod" not in repr(cliente_s3.mock_calls), "Chamada ao bucket de PRODUÇÃO detectada"


@pytest.fixture(params=MODULOS, ids=["extract", "extract_r2"])
def modulo(request):
    return importlib.import_module(request.param)


@pytest.fixture
def r2():
    return importlib.import_module("ingestion.comprasgov.extract_r2")


@pytest.fixture
def local():
    return importlib.import_module("ingestion.comprasgov.extract")


@pytest.fixture
def rodar_main(monkeypatch):
    """Executa `modulo.main()` com argv e (opcionalmente) uma sessão HTTP falsa."""

    def _rodar(mod, *argv, sessao=None):
        monkeypatch.setattr(sys, "argv", ["prog", *argv])
        if sessao is not None:
            monkeypatch.setattr(mod.requests, "Session", lambda: sessao)
        return mod.main()

    return _rodar
