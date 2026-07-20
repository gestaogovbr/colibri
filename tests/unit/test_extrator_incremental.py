from pathlib import Path

from ingestion.base.extrator.extrator_incremental import ExtratorIncremental, Tarefa
from ingestion.base.ficheiro.watermark import Watermark


def _erro_sem_watermark_no_bucket(*args, **kwargs):
    raise Exception("sem watermark anterior no bucket")


class _ExtratorFake(ExtratorIncremental):
    def __init__(self, tarefas: list[Tarefa], tmp_path: Path):
        super().__init__()
        self._tarefas = tarefas
        self.watermark = Watermark(
            nome_ficheiro="fake_watermark.csv",
            colunas_ficheiro=["ultimo_recurso", "extraido_em"],
            nome_alteracoes="fake_alteracoes.csv",
            colunas_alteracoes=["view", "periodo"],
            nome_segredo="segredo-fake",
            logger=self.logger,
        )
        self.watermark.DIRETORIO_WATERMARKS = tmp_path
        self.watermark.DIRETORIO_ALTERACOES = tmp_path

    def url_periodo(self, periodo: str) -> str:
        return f"https://exemplo.com/{periodo}"

    def chave_periodo(self, tabela: str, periodo: str) -> Path:
        return Path(tabela) / f"{periodo}.parquet"

    def gerar_tarefas(self, watermark: dict[str, dict]) -> list[Tarefa]:
        return self._tarefas


def _tarefa(identificador: str = "2021-12-01") -> Tarefa:
    return Tarefa(
        tabela="VW_FT_PNCP_COMPRA",
        identificador=identificador,
        url=f"https://exemplo.com/{identificador}",
        chave_bucket=Path(f"pncp/{identificador}.parquet"),
        alteracoes=[("VW_FT_PNCP_COMPRA", identificador)],
    )


def test_processar_arquivo_e_sempre_baixado(tmp_path, monkeypatch):
    # GIVEN: gerar_tarefas já filtrou, então não existe dedup por hash aqui
    extrator = _ExtratorFake([], tmp_path)
    monkeypatch.setattr(
        "ingestion.base.extrator.extrator_incremental.salvar_bytes_no_bucket", lambda *a, **k: None
    )
    estado: dict[str, dict] = {}

    # WHEN
    status = extrator._processar_arquivo(_tarefa(), estado, "bucket-fake", b"conteudo", b"conteudo-final")

    # THEN
    assert status == "baixado"


def test_processar_arquivo_sobrescreve_o_watermark_anterior(tmp_path, monkeypatch):
    # GIVEN
    extrator = _ExtratorFake([], tmp_path)
    monkeypatch.setattr(
        "ingestion.base.extrator.extrator_incremental.salvar_bytes_no_bucket", lambda *a, **k: None
    )
    estado = {"ultimo_recurso": {"ultimo_recurso": "VW_FT_PNCP_COMPRA:2021-11-30"}}

    # WHEN
    extrator._processar_arquivo(_tarefa(), estado, "bucket-fake", b"conteudo", b"conteudo-final")

    # THEN: só sobra a entrada nova
    assert len(estado) == 1
    assert estado["ultimo_recurso"]["ultimo_recurso"] == "VW_FT_PNCP_COMPRA:2021-12-01"


def test_executar_ingestao_processa_tarefa_e_salva_watermark(tmp_path, monkeypatch):
    # GIVEN
    extrator = _ExtratorFake([_tarefa()], tmp_path)
    monkeypatch.setattr(extrator.downloader, "obter_tipo_conteudo", lambda url: "text/csv")
    monkeypatch.setattr(extrator.downloader, "baixar_arquivo", lambda url: b"id,nome\n1,Aninha\n")
    monkeypatch.setattr(
        "ingestion.base.extrator.extrator_incremental.salvar_bytes_no_bucket", lambda *a, **k: None
    )
    monkeypatch.setattr("ingestion.base.ficheiro.ficheiro.baixar_arquivo_do_bucket", _erro_sem_watermark_no_bucket)

    # WHEN
    houve_mudanca = extrator.executar_ingestao(bucket_nome="bucket-fake")

    # THEN
    assert houve_mudanca is True
    assert extrator.watermark.caminho_ficheiro().exists()
