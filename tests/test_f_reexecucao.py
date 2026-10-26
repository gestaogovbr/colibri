"""Grupo F — reexecução, atomicidade e recuperação.
Não presume garantias: onde falta, o teste falha e é marcado `limitacao`.
"""

import hashlib
import shutil

import pytest
from apoio_delog import CSV_PADRAO, FakeSessao, caminho_saida, criar_parquet_local, executar, ler_parquet, zip_bytes


def sha(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()


def arvore(raiz):
    return sorted(str(p.relative_to(raiz)) for p in raiz.rglob("*"))


def test_duas_execucoes_criado_depois_existente(modulo):
    assert executar(modulo, FakeSessao.com_csv(CSV_PADRAO)) == "criado"
    sessao2 = FakeSessao.com_csv(CSV_PADRAO)
    assert executar(modulo, sessao2) == "existente"
    assert sessao2.chamadas == []  # 2ª execução nem baixa


def test_arquivo_existente_preservado_byte_a_byte(modulo):
    executar(modulo, FakeSessao.com_csv(CSV_PADRAO))
    antes = sha(caminho_saida())
    executar(modulo, FakeSessao.com_csv(b"x;y\n9;9\n"))  # fonte "republicada"
    assert sha(caminho_saida()) == antes


def test_caminhos_e_arvore_consistentes_entre_execucoes(modulo, raiz_exec):
    executar(modulo, FakeSessao.com_csv(CSV_PADRAO))
    antes = arvore(raiz_exec / "dados")
    executar(modulo, FakeSessao.com_csv(CSV_PADRAO))
    assert arvore(raiz_exec / "dados") == antes


def test_tipos_e_periodos_diferentes_nao_colidem(modulo):
    for tipo in ("compras", "itens", "resultados"):
        for mes in (7, 8):
            assert executar(modulo, FakeSessao.com_csv(CSV_PADRAO), tipo, 2025, mes) == "criado"
    assert len(list(caminho_saida().parents[3].rglob("*.parquet"))) == 6


def test_sem_temporarios_no_destino_nem_no_tmp_apos_sucesso(modulo, tmp_sistema, raiz_exec):
    executar(modulo, FakeSessao.com_csv(CSV_PADRAO))
    assert list(tmp_sistema.glob("colibri_delog_*")) == []
    assert [p for p in raiz_exec.rglob("*") if p.suffix in (".part", ".tmp")] == []


def test_falha_de_conversao_nao_deixa_arquivo_e_permite_nova_tentativa(modulo):
    assert executar(modulo, FakeSessao.com_csv(b"a;b;c\n1;2\n")) == "erro"
    assert not caminho_saida().exists()
    assert executar(modulo, FakeSessao.com_csv(CSV_PADRAO)) == "criado"  # nova tentativa recupera


def test_falha_de_download_nao_impede_nova_tentativa(modulo):
    assert executar(modulo, FakeSessao.status(500)) == "erro"
    assert executar(modulo, FakeSessao.com_csv(CSV_PADRAO)) == "criado"


def _copia_que_falha_no_destino_final(monkeypatch):
    """Simula disco cheio durante a gravação do Parquet final ou temporário."""
    original = shutil.copyfileobj
    estado = {"falhar": True}

    def copia(src, dst, *a, **k):
        nome = str(getattr(dst, "name", ""))

        # Extrator local: grava diretamente no .parquet
        # Extrator R2: grava primeiro em .parquet.part
        arquivo_final = nome.endswith("compras_2025_08.parquet")
        arquivo_temporario = nome.endswith(".parquet.part")

        if estado["falhar"] and (arquivo_final or arquivo_temporario):
            dst.write(src.read(10))
            raise OSError("disco cheio (simulado)")

        return original(src, dst, *a, **k)

    monkeypatch.setattr(shutil, "copyfileobj", copia)
    return estado


def test_falha_parcial_na_gravacao_final_retorna_erro(modulo, monkeypatch):
    _copia_que_falha_no_destino_final(monkeypatch)
    assert executar(modulo, FakeSessao.com_csv(CSV_PADRAO)) == "erro"


@pytest.mark.limitacao
def test_LIMITACAO_falha_parcial_nao_deveria_deixar_arquivo_incompleto(modulo, monkeypatch):
    """extract_delog_r2 não remove o arquivo parcial ('xb' sem cleanup); extract_delog remove só se tamanho==0
    ou via except BaseException interno."""
    _copia_que_falha_no_destino_final(monkeypatch)
    executar(modulo, FakeSessao.com_csv(CSV_PADRAO))
    assert not caminho_saida().exists(), "arquivo parcial ficou no destino final"


@pytest.mark.limitacao
def test_LIMITACAO_nova_tentativa_recupera_apos_falha_parcial(modulo, monkeypatch):
    estado = _copia_que_falha_no_destino_final(monkeypatch)
    executar(modulo, FakeSessao.com_csv(CSV_PADRAO))
    estado["falhar"] = False  # disco "liberado"
    assert executar(modulo, FakeSessao.com_csv(CSV_PADRAO)) == "criado"
    _, _, linhas = ler_parquet(caminho_saida())
    assert len(linhas) == 3


@pytest.mark.limitacao
@pytest.mark.parametrize("conteudo", [b"", b"PAR1-truncado"], ids=["zero_bytes", "truncado"])
def test_LIMITACAO_execucao_interrompida_deixa_arquivo_que_bloqueia_recuperacao(modulo, conteudo):
    """Simula kill -9 durante a cópia: sobra arquivo inválido. Hoje: 'existente' para sempre."""
    criar_parquet_local(conteudo=conteudo)
    assert executar(modulo, FakeSessao.com_csv(CSV_PADRAO)) == "criado"
    _, _, linhas = ler_parquet(caminho_saida())
    assert len(linhas) == 3


@pytest.mark.limitacao
def test_LIMITACAO_upload_nao_deveria_enviar_parquet_invalido(r2, cliente_s3):
    """enviar_para_r2 só checa is_file(); um Parquet corrompido/parcial iria para o R2."""
    from botocore.exceptions import ClientError

    criar_parquet_local(conteudo=b"lixo-nao-parquet")
    cliente_s3.head_object.side_effect = [ClientError({"Error": {"Code": "404", "Message": "m"}}, "HeadObject"), {}]
    assert r2.enviar_para_r2("compras", 2025, 8, "colibri-dev") == "erro"
    cliente_s3.upload_fileobj.assert_not_called()


def test_caracterizacao_republicacao_da_fonte_nao_e_detectada(modulo):
    """Caracterização: sem hash/ETag/manifesto, dado atualizado na fonte
    nunca substitui o local (ao contrário do legado)."""
    executar(modulo, FakeSessao.com_csv(b"a;b\n1;1\n"))
    executar(modulo, FakeSessao.com_zip(zip_bytes({"d.csv": b"a;b\n1;1\n2;2\n"})))
    _, _, linhas = ler_parquet(caminho_saida())
    assert len(linhas) == 1
