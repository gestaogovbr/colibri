from pathlib import Path

import pytest

from ingestion.base.extrator.extrator_auditor import ExtratorAuditor, Tarefa
from ingestion.base.ficheiro.manifesto import Manifesto


def _erro_sem_manifesto_no_bucket(*args, **kwargs):
    raise Exception("sem manifesto anterior no bucket")


class _ExtratorFake(ExtratorAuditor):
    def __init__(self, tarefas: list[Tarefa], tmp_path: Path):
        super().__init__()
        self._tarefas = tarefas
        self.manifesto = Manifesto(
            nome_ficheiro="fake_manifesto.csv",
            colunas_ficheiro=["tabela", "periodo", "hash_sha256", "num_linhas", "tamanho_bytes", "extraido_em"],
            nome_alteracoes="fake_alteracoes.csv",
            colunas_alteracoes=["tabela", "periodo"],
            nome_segredo="segredo-fake",
            logger=self.logger,
        )
        self.manifesto.DIRETORIO_FICHEIROS = tmp_path
        self.manifesto.DIRETORIO_ALTERACOES = tmp_path

    def url_periodo(self, periodo: str) -> str:
        return f"https://exemplo.com/{periodo}"

    def chave_periodo(self, tabela: str, periodo: str) -> Path:
        return Path(tabela) / f"{periodo}.parquet"

    def gerar_tarefas(self, estado: dict[str, dict]) -> list[Tarefa]:
        return self._tarefas


def _tarefa(identificador: str = "1") -> Tarefa:
    return Tarefa(
        tabela="catmats",
        identificador=identificador,
        url=f"https://exemplo.com/{identificador}",
        chave_bucket=Path(f"catmats/{identificador}.parquet"),
        alteracoes=[("catmats", identificador)],
    )


def test_processar_arquivo_baixado_quando_nao_existe_no_manifesto(tmp_path, monkeypatch):
    # GIVEN
    extrator = _ExtratorFake([], tmp_path)
    monkeypatch.setattr("ingestion.base.extrator.extrator_auditor.salvar_bytes_no_bucket", lambda *a, **k: None)
    estado: dict[str, dict] = {}

    # WHEN
    status = extrator._processar_arquivo(_tarefa(), estado, "bucket-fake", b"conteudo", b"conteudo-final")

    # THEN
    assert status == "baixado"
    assert "catmats:1" in estado


def test_processar_arquivo_atualizado_quando_hash_muda(tmp_path, monkeypatch):
    # GIVEN
    extrator = _ExtratorFake([], tmp_path)
    monkeypatch.setattr("ingestion.base.extrator.extrator_auditor.salvar_bytes_no_bucket", lambda *a, **k: None)
    estado = {"catmats:1": {"hash_sha256": "hash-antigo", "tamanho_bytes": 0, "num_linhas": 0}}

    # WHEN
    status = extrator._processar_arquivo(_tarefa(), estado, "bucket-fake", b"conteudo-novo", b"conteudo-final")

    # THEN
    assert status == "atualizado"


def test_processar_arquivo_ignorado_quando_hash_bate(tmp_path, monkeypatch):
    # GIVEN: monkeypatcha a checagem no bucket pra não depender de S3 de verdade
    extrator = _ExtratorFake([], tmp_path)
    monkeypatch.setattr(extrator, "_existe_e_bate_com_manifesto", lambda *a, **k: True)
    estado = {"catmats:1": {"hash_sha256": "hash-igual"}}

    # WHEN
    status = extrator._processar_arquivo(_tarefa(), estado, "bucket-fake", b"conteudo", b"conteudo-final")

    # THEN
    assert status == "ignorado"


def test_executar_ingestao_processa_tarefa_e_salva_manifesto(tmp_path, monkeypatch):
    # GIVEN
    extrator = _ExtratorFake([_tarefa()], tmp_path)
    monkeypatch.setattr(extrator.downloader, "obter_tipo_conteudo", lambda url: "text/csv")
    monkeypatch.setattr(extrator.downloader, "baixar_arquivo", lambda url: b"id,nome\n1,Aninha\n")
    monkeypatch.setattr("ingestion.base.extrator.extrator_auditor.salvar_bytes_no_bucket", lambda *a, **k: None)
    monkeypatch.setattr("ingestion.base.ficheiro.ficheiro.baixar_arquivo_do_bucket", _erro_sem_manifesto_no_bucket)

    # WHEN
    houve_mudanca = extrator.executar_ingestao(bucket_nome="bucket-fake")

    # THEN
    assert houve_mudanca is True
    assert extrator.manifesto.caminho_ficheiro().exists()
