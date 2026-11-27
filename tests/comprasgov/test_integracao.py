"""Integração local ponta a ponta: CSV fictício -> ZIP -> extração -> DuckDB -> Parquet -> upload mockado.

Sem DELOG e sem R2. Os testes `servico_real` ficam desativados por padrão
(COLIBRI_TESTES_SERVICO_REAL=1 para autorizar).
"""

import pytest
from botocore.exceptions import ClientError

from tests.comprasgov.apoio import FakeResposta, FakeSessao, caminho_saida, chave_r2, ler_parquet, zip_bytes

pytestmark = pytest.mark.integracao


def sessao_tres_tipos():
    corpos = {
        "compra_2025_08.csv.zip": b"cod_compra;cnpj\n001;00123456000199\n002;00000000000001\n",
        "compra_item_2025_08.csv.zip": b"cod_compra;num_item\n001;1\n001;2\n002;1\n",
        "item_resultado_2025_08.csv.zip": b"cod_compra;num_item;valor\n001;1;1,5\n",
    }
    return FakeSessao(lambda url, **kw: FakeResposta(200, zip_bytes({"x.csv": corpos[url.rsplit("/", 1)[1]]})))


def test_fluxo_local_tres_tipos(local, rodar_main):
    rodar_main(local, "--ano", "2025", "--mes", "8", sessao=sessao_tres_tipos())
    contagens = {t: len(ler_parquet(caminho_saida(t))[2]) for t in ("compras", "itens", "resultados")}
    assert contagens == {"compras": 2, "itens": 3, "resultados": 1}


def test_fluxo_com_upload_mockado(r2, rodar_main, cliente_s3):
    cliente_s3.head_object.side_effect = lambda **kw: (_ for _ in ()).throw(
        ClientError({"Error": {"Code": "404", "Message": "m"}}, "HeadObject")
    )
    enviados = []
    cliente_s3.upload_fileobj.side_effect = lambda f, b, k: (
        enviados.append((b, k)),
        setattr(cliente_s3.head_object, "side_effect", None),
    )
    rodar_main(
        r2,
        "--ano",
        "2025",
        "--mes",
        "8",
        "--tipo",
        "compras",
        "--upload",
        "--bucket",
        "colibri-dev",
        sessao=sessao_tres_tipos(),
    )
    assert enviados == [("colibri-dev", chave_r2("compras"))]


def test_reexecucao_do_fluxo_completo_e_idempotente(local, rodar_main):
    rodar_main(local, "--ano", "2025", "--mes", "8", sessao=sessao_tres_tipos())
    sessao2 = sessao_tres_tipos()
    rodar_main(local, "--ano", "2025", "--mes", "8", sessao=sessao2)
    assert sessao2.chamadas == []


def test_cli_retorna_1_se_algum_tipo_falha(local, rodar_main):
    with pytest.raises(SystemExit) as e:
        rodar_main(local, "--ano", "2025", "--mes", "8", sessao=FakeSessao.status(500))
    assert e.value.code == 1


def test_cli_404_em_tudo_nao_e_erro(local, rodar_main):
    rodar_main(local, "--ano", "2025", "--mes", "9", sessao=FakeSessao.status(404))  # sem SystemExit


@pytest.mark.servico_real
def test_real_head_comprasgov_agosto_2025():
    """Opt-in, somente leitura (HEAD): confirma que a URL de referência ainda responde."""
    import requests

    r = requests.head(
        "https://repositorio.dados.gov.br/seges/comprasgov/pncp/2025/08/compra_2025_08.csv.zip",
        timeout=30,
        allow_redirects=True,
    )
    assert r.status_code == 200
