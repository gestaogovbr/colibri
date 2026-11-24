"""Testes do `colibri bucket clone` com um cliente S3 falso, em memória.

Não usam credenciais nem rede: `cli._cliente` é trocado por `FakeS3`, que guarda
os objetos num dicionário e registra cada chamada, para os testes verificarem o
que foi copiado, apagado e em que ordem.
"""

import json
import re
import threading
from datetime import datetime

import pytest
from botocore.exceptions import ClientError
from click.testing import CliRunner
from rich.console import Console

import cli

GB = 1024**3


class FakeS3:
    def __init__(self, buckets: dict, falhas: dict | None = None, pode_listar: bool = True):
        # buckets: {bucket: {chave: {"Size": int, "ETag": str, "Metadata": dict}}}
        self.buckets = {nome: {k: {"Metadata": {}, **v} for k, v in objs.items()} for nome, objs in buckets.items()}
        # falhas: {(operação, chave): exceção}; operação em {"head", "copy", "delete"}
        self.falhas = falhas or {}
        self.pode_listar = pode_listar
        self.chamadas = []
        self._lock = threading.Lock()

    def _registrar(self, operacao, chave, **detalhes):
        with self._lock:
            self.chamadas.append((operacao, chave, detalhes))
        for alvo in ((operacao, chave), (operacao, "*")):
            if alvo in self.falhas:
                raise self.falhas[alvo]

    def list_buckets(self):
        if not self.pode_listar:
            raise ClientError({"Error": {"Code": "AccessDenied"}}, "ListBuckets")
        return {"Buckets": [{"Name": nome} for nome in self.buckets]}

    def get_paginator(self, _nome):
        fake = self

        class Paginador:
            def paginate(self, Bucket, Prefix):
                yield {
                    "Contents": [
                        {"Key": k, "Size": o["Size"], "ETag": o["ETag"]}
                        for k, o in fake.buckets[Bucket].items()
                        if k.startswith(Prefix)
                    ]
                }

        return Paginador()

    def head_object(self, Bucket, Key):
        self._registrar("head", Key, bucket=Bucket)
        obj = self.buckets[Bucket][Key]
        return {"ContentType": "application/octet-stream", "Metadata": dict(obj["Metadata"])}

    def copy_object(self, Bucket, Key, CopySource, MetadataDirective=None, **extra):
        self._registrar("copy", Key, metodo="copy_object", diretiva=MetadataDirective)
        origem = self.buckets[CopySource["Bucket"]][Key]
        if MetadataDirective == "REPLACE":
            # Como no S3: a cópia de um objeto multipart vira um objeto único, com outro ETag
            self.buckets[Bucket][Key] = {"Size": origem["Size"], "ETag": f'"md5-{Key}"', "Metadata": extra["Metadata"]}
        else:
            self.buckets[Bucket][Key] = dict(origem)

    def copy(self, CopySource, Bucket, Key, ExtraArgs, Config):
        self._registrar("copy", Key, metodo="s3.copy")
        origem = self.buckets[CopySource["Bucket"]][Key]
        self.buckets[Bucket][Key] = {"Size": origem["Size"], "ETag": '"copia-999"', "Metadata": ExtraArgs["Metadata"]}

    def delete_objects(self, Bucket, Delete):
        chaves = [o["Key"] for o in Delete["Objects"]]
        for chave in chaves:
            self._registrar("delete", chave)
        for chave in chaves:
            del self.buckets[Bucket][chave]
        return {}

    def put_object(self, Bucket, Key, Body, ContentType):
        self._registrar("put", Key)
        self.buckets[Bucket][Key] = {"Size": len(Body), "ETag": '"registro"', "Metadata": {}, "Body": Body}

    def get_object(self, Bucket, Key):
        if Key not in self.buckets[Bucket]:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        corpo = self.buckets[Bucket][Key]["Body"]

        class Corpo:
            def read(self):
                return corpo

        return {"Body": Corpo()}

    def chaves_historico(self, bucket, origem=""):
        prefixo = f"{cli.PREFIXO_REGISTRO_CLONES}{origem}"
        return sorted(k for k in self.buckets[bucket] if k.startswith(prefixo))

    def historico(self, bucket, origem=""):
        """Registros de clonagem do bucket, do mais antigo para o mais recente"""
        return [json.loads(self.buckets[bucket][k]["Body"]) for k in self.chaves_historico(bucket, origem)]

    def operacoes(self, *tipos):
        return [(op, chave) for op, chave, _ in self.chamadas if op in tipos]


@pytest.fixture
def rodar(monkeypatch):
    def _rodar(fake, args, input=None, interativo=False):
        monkeypatch.setattr(cli, "_cliente", lambda *a, **k: fake)
        monkeypatch.setattr(cli, "_interativo", lambda: interativo)
        return CliRunner().invoke(cli.cli, ["bucket", "clone", *args], input=input)

    return _rodar


def _buckets_padrao():
    return {
        "colibri-prod": {
            "igual.txt": {"Size": 10, "ETag": '"a"'},
            "novo.txt": {"Size": 20, "ETag": '"b"'},
            "mudou.txt": {"Size": 30, "ETag": '"c"'},
        },
        "colibri-dev": {
            "igual.txt": {"Size": 10, "ETag": '"a"'},
            "mudou.txt": {"Size": 30, "ETag": '"velho"'},
            "sobra.txt": {"Size": 1, "ETag": '"s"'},
        },
    }


# --- Proteção contra perda de dados ---


def test_apaga_sobras_so_depois_de_todas_as_copias(rodar):
    fake = FakeS3(_buckets_padrao())
    resultado = rodar(fake, ["--yes"])

    assert resultado.exit_code == 0, resultado.output
    ops = [op for op, _ in fake.operacoes("copy", "delete")]
    assert ops == ["copy", "copy", "delete"]
    dados_destino = {k for k in fake.buckets["colibri-dev"] if not k.startswith(cli.PREFIXO_REGISTRO_CLONES)}
    assert dados_destino == fake.buckets["colibri-prod"].keys()


def test_destino_producao_exige_nome_mesmo_com_yes(rodar):
    fake = FakeS3(_buckets_padrao())
    resultado = rodar(fake, ["colibri-dev", "colibri-prod", "--yes"], input="sim\n")

    assert "Operação cancelada" in resultado.output
    assert fake.operacoes("copy", "delete") == []


def test_destino_producao_com_nome_correto_executa(rodar):
    fake = FakeS3(_buckets_padrao())
    resultado = rodar(fake, ["colibri-dev", "colibri-prod", "--yes"], input="colibri-prod\n")

    assert resultado.exit_code == 0, resultado.output
    assert fake.operacoes("copy", "delete")


def test_dry_run_nao_altera_nada(rodar):
    fake = FakeS3(_buckets_padrao())
    resultado = rodar(fake, ["--dry-run"])

    assert resultado.exit_code == 0, resultado.output
    assert "nada foi alterado" in resultado.output
    assert fake.operacoes("copy", "delete") == []


def test_origem_vazia_nao_apaga_destino(rodar):
    fake = FakeS3({"colibri-prod": {}, "colibri-dev": {"x.txt": {"Size": 1, "ETag": '"x"'}}})
    resultado = rodar(fake, ["--yes"])

    assert "Nada para clonar" in resultado.output
    assert fake.operacoes("copy", "delete") == []
    assert "x.txt" in fake.buckets["colibri-dev"]


def test_origem_igual_ao_destino_e_recusada(rodar):
    fake = FakeS3(_buckets_padrao())
    resultado = rodar(fake, ["colibri-dev", "colibri-dev", "--yes"])

    assert "não podem ser o mesmo bucket" in resultado.output
    assert fake.operacoes("copy", "delete") == []


def test_sem_confirmacao_cancela(rodar):
    fake = FakeS3(_buckets_padrao())
    resultado = rodar(fake, [], input="n\n")

    assert "Operação cancelada" in resultado.output
    assert fake.operacoes("copy", "delete") == []


def test_prefixo_restringe_copia_e_exclusao(rodar):
    fake = FakeS3(
        {
            "colibri-prod": {"lake/a.parquet": {"Size": 1, "ETag": '"a"'}, "outro/b.txt": {"Size": 1, "ETag": '"b"'}},
            "colibri-dev": {
                "lake/sobra.parquet": {"Size": 1, "ETag": '"s"'},
                "outro/c.txt": {"Size": 1, "ETag": '"c"'},
            },
        }
    )
    resultado = rodar(fake, ["--prefixo", "lake/", "--yes"])

    assert resultado.exit_code == 0, resultado.output
    assert set(fake.operacoes("copy", "delete")) == {("copy", "lake/a.parquet"), ("delete", "lake/sobra.parquet")}
    assert "outro/c.txt" in fake.buckets["colibri-dev"]


# --- Arquivos iguais são pulados ---


def test_pula_arquivo_com_mesmo_tamanho_e_etag(rodar):
    fake = FakeS3(_buckets_padrao())
    rodar(fake, ["--yes"])

    copiados = {chave for _, chave in fake.operacoes("copy")}
    assert copiados == {"novo.txt", "mudou.txt"}


def test_pula_arquivo_com_etag_original_no_metadado(rodar):
    fake = FakeS3(
        {
            "colibri-prod": {"a.parquet": {"Size": 10, "ETag": '"x-2"'}},
            "colibri-dev": {
                "a.parquet": {"Size": 10, "ETag": '"md5"', "Metadata": {cli.META_ETAG_ORIGEM: "x-2"}},
            },
        }
    )
    resultado = rodar(fake, ["--yes"])

    assert fake.operacoes("copy") == []
    assert "já está igual" in resultado.output


def test_copia_quando_tamanho_igual_mas_conteudo_diferente(rodar):
    fake = FakeS3(
        {
            "colibri-prod": {"a.parquet": {"Size": 10, "ETag": '"x-2"'}},
            "colibri-dev": {"a.parquet": {"Size": 10, "ETag": '"md5"', "Metadata": {cli.META_ETAG_ORIGEM: "outro-3"}}},
        }
    )
    rodar(fake, ["--yes"])

    assert fake.operacoes("copy") == [("copy", "a.parquet")]


def test_segunda_execucao_nao_copia_nada(rodar):
    buckets = _buckets_padrao()
    buckets["colibri-prod"]["multi.parquet"] = {"Size": 100, "ETag": '"d-3"'}
    buckets["colibri-prod"]["grande.bin"] = {"Size": 6 * GB, "ETag": '"e-700"'}
    fake = FakeS3(buckets)
    rodar(fake, ["--yes"])
    fake.chamadas.clear()

    resultado = rodar(fake, ["--yes"])

    assert fake.operacoes("copy", "delete") == []
    assert "já está igual" in resultado.output


# --- Forma de cópia ---


def _metodos_de_copia(fake):
    return {chave: (d.get("metodo"), d.get("diretiva")) for op, chave, d in fake.chamadas if op == "copy"}


def test_escolhe_forma_de_copia_por_tamanho_e_etag(rodar):
    fake = FakeS3(
        {
            "colibri-prod": {
                "simples.txt": {"Size": 10, "ETag": '"a"'},
                "multi.parquet": {"Size": 100, "ETag": '"b-3"'},
                "limite.bin": {"Size": 5 * GB, "ETag": '"c"'},
                "grande.bin": {"Size": 5 * GB + 1, "ETag": '"d-700"'},
            },
            "colibri-dev": {},
        }
    )
    rodar(fake, ["--yes"])

    assert _metodos_de_copia(fake) == {
        "simples.txt": ("copy_object", None),
        "multi.parquet": ("copy_object", "REPLACE"),
        "limite.bin": ("copy_object", None),
        "grande.bin": ("s3.copy", None),
    }
    destino = fake.buckets["colibri-dev"]
    assert destino["multi.parquet"]["Metadata"][cli.META_ETAG_ORIGEM] == "b-3"
    assert destino["grande.bin"]["Metadata"][cli.META_ETAG_ORIGEM] == "d-700"
    assert cli.META_ETAG_ORIGEM not in destino["simples.txt"]["Metadata"]


# --- Falhas isoladas ---


def test_falha_na_copia_nao_interrompe_as_outras(rodar):
    fake = FakeS3(_buckets_padrao(), falhas={("copy", "novo.txt"): ConnectionError("rede caiu")})
    resultado = rodar(fake, ["--yes"])

    assert resultado.exit_code == 0, resultado.output
    assert "Falha ao copiar novo.txt" in resultado.output
    assert "1 arquivo(s) falharam" in resultado.output
    assert fake.buckets["colibri-dev"]["mudou.txt"]["ETag"] == '"c"'
    assert "sobra.txt" not in fake.buckets["colibri-dev"]


def test_falha_na_comparacao_copia_o_arquivo_de_novo(rodar):
    fake = FakeS3(
        {
            "colibri-prod": {"a.parquet": {"Size": 10, "ETag": '"x-2"'}},
            "colibri-dev": {"a.parquet": {"Size": 10, "ETag": '"md5"'}},
        },
        falhas={("head", "a.parquet"): ConnectionError("rede caiu")},
    )
    # A falha vale para qualquer head de a.parquet; liberamos o da origem depois da comparação
    original = fake.head_object

    def head_object(Bucket, Key):
        if Bucket == "colibri-prod":
            return {"ContentType": "application/octet-stream", "Metadata": {}}
        return original(Bucket=Bucket, Key=Key)

    fake.head_object = head_object
    resultado = rodar(fake, ["--yes"])

    assert resultado.exit_code == 0, resultado.output
    assert fake.operacoes("copy") == [("copy", "a.parquet")]


def test_falha_na_exclusao_nao_interrompe_o_comando(rodar):
    fake = FakeS3(_buckets_padrao(), falhas={("delete", "sobra.txt"): ConnectionError("rede caiu")})
    resultado = rodar(fake, ["--yes"])

    assert resultado.exit_code == 0, resultado.output
    assert "Falha ao apagar sobra.txt" in resultado.output
    assert "De olho no faturamento" in resultado.output


# --- Escolha interativa ---


def test_escolha_interativa_por_numero_e_por_nome(rodar, monkeypatch):
    monkeypatch.setattr(cli, "_listar_segredos", lambda: ["colibri-token-desenvolvedor", "outro-token"])
    fake = FakeS3(
        {
            "colibri-prod": {},
            "colibri-dev": {},
            "origem-x": {"a.txt": {"Size": 1, "ETag": '"a"'}},
            "destino-y": {},
        }
    )
    # segredo: padrão (Enter); origem: por nome; destino: por nome; confirmação
    resultado = rodar(fake, [], input="\norigem-x\ndestino-y\ny\n", interativo=True)

    assert resultado.exit_code == 0, resultado.output
    assert "a.txt" in fake.buckets["destino-y"]


def test_lista_buckets_conhecidos_quando_list_buckets_e_negado(rodar, monkeypatch):
    monkeypatch.setattr(cli, "_listar_segredos", lambda: ["colibri-token-desenvolvedor"])
    fake = FakeS3(
        {"colibri-prod": {}, "colibri-dev": {}, "colibri-arquivos": {"a.txt": {"Size": 1, "ETag": '"a"'}}},
        pode_listar=False,
    )
    # origem: opção 3 da lista de buckets conhecidos (colibri-arquivos); destino: padrão (colibri-dev)
    resultado = rodar(fake, [], input="\n3\n\ny\n", interativo=True)

    assert resultado.exit_code == 0, resultado.output
    assert "colibri-arquivos" in resultado.output
    assert "a.txt" in fake.buckets["colibri-dev"]


def test_opcao_invalida_pergunta_de_novo(rodar, monkeypatch):
    monkeypatch.setattr(cli, "_listar_segredos", lambda: ["colibri-token-desenvolvedor"])
    fake = FakeS3({"colibri-prod": {}, "colibri-dev": {}})
    resultado = rodar(fake, ["colibri-prod", "colibri-dev"], input="9\n1\n", interativo=True)

    assert "Opção inválida" in resultado.output


# --- Histórico de clonagens ---


def test_grava_registro_no_historico_ao_terminar(rodar):
    fake = FakeS3(_buckets_padrao())
    rodar(fake, ["--yes"])

    [chave] = fake.chaves_historico("colibri-dev")
    assert re.fullmatch(
        r"_colibri_clones/colibri-prod/\d{4}-\d\d-\d\dT\d\d-\d\d-\d\d\.\d{6}Z-[0-9a-f]{6}\.json", chave
    )
    [registro] = fake.historico("colibri-dev")
    assert registro["origem"] == "colibri-prod"
    assert registro["destino"] == "colibri-dev"
    assert registro["prefixo"] == ""
    assert (registro["copiados"], registro["pulados"], registro["apagados"]) == (2, 1, 1)
    assert registro["bytes_copiados"] == 50
    assert registro["falhas_copia"] == registro["falhas_exclusao"] == []
    assert registro["usuario"]
    assert datetime.fromisoformat(registro["iniciado_em"]) <= datetime.fromisoformat(registro["concluido_em"])
    # O registro é gravado depois de todas as cópias e exclusões
    assert fake.operacoes("copy", "delete", "put")[-1] == ("put", chave)


def test_historico_acumula_clonagens_sem_sobrescrever(rodar):
    fake = FakeS3(_buckets_padrao())
    rodar(fake, ["--yes"])
    fake.buckets["colibri-prod"]["outro.txt"] = {"Size": 7, "ETag": '"o"', "Metadata": {}}
    rodar(fake, ["--yes"])

    historico = fake.historico("colibri-dev", "colibri-prod")
    assert [r["copiados"] for r in historico] == [2, 1]


def test_historico_separado_por_origem(rodar):
    buckets = _buckets_padrao()
    buckets["colibri-arquivos"] = {"a.txt": {"Size": 1, "ETag": '"a"'}}
    fake = FakeS3(buckets)
    rodar(fake, ["colibri-prod", "colibri-dev", "--yes"])
    rodar(fake, ["colibri-arquivos", "colibri-dev", "--yes"])

    assert [r["origem"] for r in fake.historico("colibri-dev", "colibri-prod/")] == ["colibri-prod"]
    assert [r["origem"] for r in fake.historico("colibri-dev", "colibri-arquivos/")] == ["colibri-arquivos"]


def test_registro_lista_as_falhas(rodar):
    fake = FakeS3(
        _buckets_padrao(),
        falhas={("copy", "novo.txt"): ConnectionError("x"), ("delete", "sobra.txt"): ConnectionError("y")},
    )
    rodar(fake, ["--yes"])

    [registro] = fake.historico("colibri-dev")
    assert registro["falhas_copia"] == ["novo.txt"]
    assert registro["falhas_exclusao"] == ["sobra.txt"]
    assert (registro["copiados"], registro["apagados"]) == (1, 0)


def test_historico_nunca_e_copiado_nem_apagado(rodar):
    da_origem = f"{cli.PREFIXO_REGISTRO_CLONES}outro-bucket/2026-01-01T00-00-00Z-aaaaaa.json"
    do_destino = f"{cli.PREFIXO_REGISTRO_CLONES}colibri-prod/2026-01-01T00-00-00Z-bbbbbb.json"
    buckets = _buckets_padrao()
    buckets["colibri-prod"][da_origem] = {"Size": 2, "ETag": '"r"', "Body": b"{}"}
    buckets["colibri-dev"][do_destino] = {"Size": 2, "ETag": '"r"', "Body": b"{}"}
    fake = FakeS3(buckets)
    resultado = rodar(fake, ["--yes"])

    assert resultado.exit_code == 0, resultado.output
    chaves = {chave for _, chave in fake.operacoes("copy", "delete")}
    assert not any(c.startswith(cli.PREFIXO_REGISTRO_CLONES) for c in chaves)
    assert "2 arquivo(s) serão copiados" in resultado.output
    assert do_destino in fake.buckets["colibri-dev"]
    assert da_origem not in fake.buckets["colibri-dev"]
    assert len(fake.chaves_historico("colibri-dev", "colibri-prod/")) == 2


@pytest.mark.parametrize(
    ("args", "entrada"),
    [(["--dry-run"], None), ([], "n\n"), (["colibri-dev", "colibri-prod", "--yes"], "errado\n")],
    ids=["dry-run", "cancelado", "producao-cancelado"],
)
def test_nao_grava_registro_sem_alteracao(rodar, args, entrada):
    fake = FakeS3(_buckets_padrao())
    rodar(fake, args, input=entrada)

    assert fake.operacoes("put") == []


def test_nao_grava_registro_quando_destino_ja_esta_igual(rodar):
    fake = FakeS3(_buckets_padrao())
    rodar(fake, ["--yes"])
    fake.chamadas.clear()

    rodar(fake, ["--yes"])

    assert fake.operacoes("put") == []


def test_falha_ao_gravar_registro_nao_falha_o_clone(rodar):
    fake = FakeS3(_buckets_padrao(), falhas={("put", "*"): ConnectionError("rede caiu")})
    resultado = rodar(fake, ["--yes"])

    assert resultado.exit_code == 0, resultado.output
    assert "Não foi possível gravar o registro" in resultado.output
    assert "2 arquivo(s) copiado(s)" in resultado.output


# --- clone-status ---


def _status(monkeypatch, fake, args):
    monkeypatch.setattr(cli, "_cliente", lambda *a, **k: fake)
    # Terminal largo o bastante para a tabela não cortar as colunas
    monkeypatch.setattr(cli, "console", Console(width=200))
    return CliRunner().invoke(cli.cli, ["bucket", "clone-status", *args])


def _fake_com_duas_origens(rodar):
    buckets = _buckets_padrao()
    buckets["colibri-arquivos"] = {"a.txt": {"Size": 1, "ETag": '"a"'}}
    fake = FakeS3(buckets, falhas={("copy", "novo.txt"): ConnectionError("x")})
    rodar(fake, ["--yes"])
    fake.falhas.clear()
    rodar(fake, ["--yes"])
    rodar(fake, ["colibri-arquivos", "colibri-dev", "--yes"])
    return fake


def test_clone_status_mostra_historico_por_origem(rodar, monkeypatch):
    fake = _fake_com_duas_origens(rodar)
    resultado = _status(monkeypatch, fake, [])

    assert resultado.exit_code == 0, resultado.output
    assert "Clonagens de colibri-arquivos para colibri-dev" in resultado.output
    assert "Clonagens de colibri-prod para colibri-dev" in resultado.output
    assert "2 de 2 clonagem(ns)" in resultado.output
    assert "1 de 1 clonagem(ns)" in resultado.output
    assert "bucket inteiro" in resultado.output


def test_clone_status_filtra_origem_e_limita(rodar, monkeypatch):
    fake = _fake_com_duas_origens(rodar)
    resultado = _status(monkeypatch, fake, ["colibri-dev", "--origem", "colibri-prod", "--limite", "1"])

    assert resultado.exit_code == 0, resultado.output
    assert "Clonagens de colibri-prod para colibri-dev" in resultado.output
    assert "colibri-arquivos" not in resultado.output
    assert "1 de 2 clonagem(ns)" in resultado.output


def test_clone_status_sem_registro(monkeypatch):
    fake = FakeS3({"colibri-dev": {}})
    resultado = _status(monkeypatch, fake, ["colibri-dev"])

    assert resultado.exit_code == 0, resultado.output
    assert "Nenhuma clonagem registrada em colibri-dev" in resultado.output


def test_clone_status_avisa_registro_ilegivel(monkeypatch):
    chave = f"{cli.PREFIXO_REGISTRO_CLONES}colibri-prod/2026-01-01T00-00-00Z-aaaaaa.json"
    fake = FakeS3({"colibri-dev": {chave: {"Size": 3, "ETag": '"x"', "Body": b"???"}}})
    resultado = _status(monkeypatch, fake, [])

    assert resultado.exit_code == 0, resultado.output
    assert "Registro ilegível" in resultado.output
