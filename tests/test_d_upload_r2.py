"""Grupo D — upload para o R2 (boto3 sempre mockado)."""

import pytest
from apoio_delog import FakeSessao, chave_r2, criar_parquet_local
from botocore.exceptions import ClientError, EndpointConnectionError, NoCredentialsError


def erro_cliente(codigo, op="HeadObject"):
    return ClientError({"Error": {"Code": codigo, "Message": "msg"}}, op)


@pytest.fixture
def arquivo_local():
    return criar_parquet_local("compras", 2025, 8)


def test_chave_remota_no_upload(r2, cliente_s3, arquivo_local):
    cliente_s3.head_object.side_effect = [erro_cliente("404"), {}]
    assert r2.enviar_para_r2("compras", 2025, 8, "colibri-dev") == "enviado"
    args = cliente_s3.upload_fileobj.call_args.args
    assert args[1] == "colibri-dev"
    assert args[2] == chave_r2("compras", 2025, 8)


def test_upload_bem_sucedido_confirma_com_head_object(r2, cliente_s3, arquivo_local):
    cliente_s3.head_object.side_effect = [erro_cliente("404"), {}]
    assert r2.enviar_para_r2("compras", 2025, 8, "colibri-dev") == "enviado"
    assert cliente_s3.head_object.call_count == 2  # antes (existe?) e depois (confirmação)
    cliente_s3.upload_fileobj.assert_called_once()


@pytest.mark.parametrize("codigo", ["404", "NoSuchKey", "NotFound"])
def test_codigos_de_nao_encontrado_disparam_upload(r2, cliente_s3, arquivo_local, codigo):
    cliente_s3.head_object.side_effect = [erro_cliente(codigo), {}]
    assert r2.enviar_para_r2("compras", 2025, 8, "colibri-dev") == "enviado"


def test_objeto_ja_existente_nao_reenvia(r2, cliente_s3, arquivo_local):
    assert r2.enviar_para_r2("compras", 2025, 8, "colibri-dev") == "existente"
    cliente_s3.upload_fileobj.assert_not_called()


@pytest.mark.parametrize("bucket", ["colibri-prod", "colibri-arquivos", "outro", "", None])
def test_bucket_diferente_do_dev_e_recusado_antes_de_qualquer_acesso(
    r2, cliente_s3, criar_cliente_mock, carregar_segredo_mock, arquivo_local, bucket
):
    with pytest.raises(ValueError):
        r2.enviar_para_r2("compras", 2025, 8, bucket)
    criar_cliente_mock.assert_not_called()
    carregar_segredo_mock.assert_not_called()
    assert cliente_s3.mock_calls == []


def test_bucket_dev_usado_em_todas_as_chamadas(r2, cliente_s3, arquivo_local):
    cliente_s3.head_object.side_effect = [erro_cliente("404"), {}]
    r2.enviar_para_r2("compras", 2025, 8, "colibri-dev")
    buckets = [c.kwargs.get("Bucket") for c in cliente_s3.head_object.call_args_list]
    buckets.append(cliente_s3.upload_fileobj.call_args.args[1])
    assert set(buckets) == {"colibri-dev"}


@pytest.mark.parametrize("codigo", ["403", "InvalidAccessKeyId", "SignatureDoesNotMatch", "AccessDenied"])
def test_falha_de_autenticacao_no_head_retorna_erro_sem_upload(r2, cliente_s3, arquivo_local, codigo):
    cliente_s3.head_object.side_effect = erro_cliente(codigo)
    assert r2.enviar_para_r2("compras", 2025, 8, "colibri-dev") == "erro"
    cliente_s3.upload_fileobj.assert_not_called()


def test_segredo_ausente_propaga_excecao(r2, carregar_segredo_mock, cliente_s3, arquivo_local):
    """Caracterização: falha ao carregar segredos NÃO é capturada (estoura em vez de 'erro')."""
    carregar_segredo_mock.side_effect = FileNotFoundError(".segredos.yml")
    with pytest.raises(FileNotFoundError):
        r2.enviar_para_r2("compras", 2025, 8, "colibri-dev")
    assert cliente_s3.mock_calls == []


@pytest.mark.limitacao
@pytest.mark.parametrize(
    "exc",
    [NoCredentialsError(), EndpointConnectionError(endpoint_url="https://x")],
    ids=["sem_credenciais", "sem_rede"],
)
def test_LIMITACAO_erro_botocore_nao_clienterror_no_head_deveria_virar_erro(r2, cliente_s3, arquivo_local, exc):
    """O 1º try só captura ClientError; NoCredentialsError/EndpointConnectionError escapam e derrubam o main."""
    cliente_s3.head_object.side_effect = exc
    assert r2.enviar_para_r2("compras", 2025, 8, "colibri-dev") == "erro"


def test_falha_no_upload_via_salvar_arquivo_retorna_erro(r2, cliente_s3, arquivo_local, monkeypatch):
    cliente_s3.head_object.side_effect = [erro_cliente("404"), {}]
    cliente_s3.upload_fileobj.side_effect = erro_cliente("InternalError", "PutObject")
    assert r2.enviar_para_r2("compras", 2025, 8, "colibri-dev") == "erro"


def test_falha_generica_na_funcao_de_upload_retorna_erro(r2, cliente_s3, arquivo_local, monkeypatch):
    cliente_s3.head_object.side_effect = erro_cliente("404")
    monkeypatch.setattr(r2, "salvar_arquivo_no_bucket", lambda **kw: (_ for _ in ()).throw(OSError("disco")))
    assert r2.enviar_para_r2("compras", 2025, 8, "colibri-dev") == "erro"


def test_falha_na_confirmacao_pos_upload_retorna_erro(r2, cliente_s3, arquivo_local):
    cliente_s3.head_object.side_effect = [erro_cliente("404"), erro_cliente("404")]
    assert r2.enviar_para_r2("compras", 2025, 8, "colibri-dev") == "erro"
    cliente_s3.upload_fileobj.assert_called_once()


def test_arquivo_local_inexistente_retorna_erro_sem_tocar_r2(r2, cliente_s3, criar_cliente_mock):
    assert r2.enviar_para_r2("compras", 2025, 8, "colibri-dev") == "erro"
    criar_cliente_mock.assert_not_called()
    assert cliente_s3.mock_calls == []


def test_diretorio_no_lugar_do_arquivo_retorna_erro(r2, cliente_s3):
    from apoio_delog import caminho_saida

    caminho_saida().mkdir(parents=True)
    assert r2.enviar_para_r2("compras", 2025, 8, "colibri-dev") == "erro"


# ---- CLI: comportamento com/sem --upload ----


def test_sem_upload_nao_chama_r2(r2, rodar_main, monkeypatch, cliente_s3, criar_cliente_mock):
    monkeypatch.setattr(r2, "processar_tipo", lambda *a, **k: "criado")
    rodar_main(r2, "--ano", "2025", "--mes", "8", sessao=FakeSessao.status(404))
    criar_cliente_mock.assert_not_called()
    assert cliente_s3.mock_calls == []


def test_bucket_sem_upload_e_rejeitado(r2, rodar_main):
    with pytest.raises(SystemExit) as e:
        rodar_main(r2, "--ano", "2025", "--mes", "8", "--bucket", "colibri-dev", sessao=FakeSessao.status(404))
    assert e.value.code == 2


@pytest.mark.parametrize("extra", [[], ["--bucket", "colibri-prod"], ["--bucket", "outro"]])
def test_upload_exige_bucket_dev(r2, rodar_main, cliente_s3, extra):
    with pytest.raises(SystemExit) as e:
        rodar_main(r2, "--ano", "2025", "--mes", "8", "--upload", *extra, sessao=FakeSessao.status(404))
    assert e.value.code == 2
    assert cliente_s3.mock_calls == []


def test_upload_nao_tenta_enviar_quando_extracao_indisponivel_ou_erro(r2, rodar_main, monkeypatch):
    chamadas = []
    monkeypatch.setattr(
        r2, "processar_tipo", lambda s, a, m, t: {"compras": "indisponivel", "itens": "erro"}.get(t, "erro")
    )
    monkeypatch.setattr(r2, "enviar_para_r2", lambda *a: chamadas.append(a) or "enviado")
    with pytest.raises(SystemExit) as e:
        rodar_main(
            r2, "--ano", "2025", "--mes", "8", "--upload", "--bucket", "colibri-dev", sessao=FakeSessao.status(404)
        )
    assert e.value.code == 1
    assert chamadas == []


def test_main_sai_com_1_se_upload_falhar(r2, rodar_main, monkeypatch):
    monkeypatch.setattr(r2, "processar_tipo", lambda *a: "criado")
    monkeypatch.setattr(r2, "enviar_para_r2", lambda *a: "erro")
    with pytest.raises(SystemExit) as e:
        rodar_main(
            r2, "--ano", "2025", "--mes", "8", "--upload", "--bucket", "colibri-dev", sessao=FakeSessao.status(404)
        )
    assert e.value.code == 1


def test_main_sem_erros_nao_levanta_systemexit(r2, rodar_main, monkeypatch):
    monkeypatch.setattr(r2, "processar_tipo", lambda *a: "criado")
    monkeypatch.setattr(r2, "enviar_para_r2", lambda *a: "enviado")
    rodar_main(r2, "--ano", "2025", "--mes", "8", "--upload", "--bucket", "colibri-dev", sessao=FakeSessao.status(404))


@pytest.mark.limitacao
def test_LIMITACAO_erro_inesperado_em_um_tipo_nao_deveria_abortar_os_outros(r2, rodar_main, monkeypatch):
    """Exceção não tratada em enviar_para_r2 interrompe o loop: tipos seguintes nem são processados."""
    from botocore.exceptions import NoCredentialsError

    visitados = []

    def fake_enviar(tipo, *a):
        visitados.append(tipo)
        raise NoCredentialsError()

    monkeypatch.setattr(r2, "processar_tipo", lambda *a: "criado")
    monkeypatch.setattr(r2, "enviar_para_r2", fake_enviar)
    with pytest.raises(SystemExit):
        rodar_main(
            r2, "--ano", "2025", "--mes", "8", "--upload", "--bucket", "colibri-dev", sessao=FakeSessao.status(404)
        )
    assert visitados == ["compras", "itens", "resultados"]
