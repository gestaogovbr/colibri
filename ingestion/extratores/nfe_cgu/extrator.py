import io
from typing import override
import zipfile
from datetime import date
from pathlib import Path

from tqdm import tqdm

from ingestion.base.extrator.extrator_auditor import ExtratorAuditor, Tarefa
from ingestion.base.ficheiro.manifesto import Manifesto


class ExtratorNfeCgu(ExtratorAuditor):
    """
    NFe do Portal da Transparência (CGU): 
        .Implementação envolve somente *sobrescrever* `url_periodo`, `chave_periodo` e `gerar_tarefas`;
        .1 ZIP por período (YYYY-MM) com 3 tabelas (itens, eventos, nf).
    """

    URL_BASE = "https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/nfe"

    def __init__(self):
        super().__init__()

        self.periodo_inicio = date(2022, 1, 1)

        self.manifesto = Manifesto(
            nome_ficheiro="nfe_cgu_manifesto.csv",
            colunas_ficheiro=[
                "tabela",
                "periodo",
                "hash_sha256",
                "num_linhas",
                "tamanho_bytes",
                "extraido_em",
            ],
            nome_alteracoes="nfe_cgu_alteracoes.csv",
            colunas_alteracoes=["tabela", "periodo"],
            nome_segredo=self.NOME_SEGREDO,
            logger=self.logger,
        )


    @override
    def url_periodo(self, periodo: str) -> str:
        return f"{self.URL_BASE}/{periodo.replace('-', '')}_NFe.zip"


    @override
    def chave_periodo(self, tabela: str, periodo: str) -> Path:
        return Path("nfe_cgu") / tabela / f"{periodo}.parquet"


    @override
    def gerar_tarefas(self, estado: dict[str, dict]) -> list[Tarefa]:
        """
        3 tarefas (itens, eventos, nf) por período (YYYY-MM), do início até o mês anterior ao atual
        """
        hoje = date.today()
        periodos: list[str] = []

        ano, mes = self.periodo_inicio.year, self.periodo_inicio.month
        while (ano, mes) < (hoje.year, hoje.month):
            periodos.append(f"{ano}-{mes:02d}")
            mes += 1
            if mes > 12:
                mes, ano = 1, ano + 1

        tarefas: list[Tarefa] = []
        for periodo in tqdm(periodos, desc="nfe_cgu: baixando ZIPs"):
            url = self.url_periodo(periodo)
            # Cada zip leva 3 tabelas, sob uma mesma entrada no manifesto (o zip)
            tarefas.append(Tarefa(
                tabela="NFe-CGU",
                identificador=periodo,
                url=url,
                chave_bucket=self.chave_periodo("zip", periodo),
                alteracoes=[("zip", periodo)],
            ))

        return tarefas


if __name__ == "__main__":
    ExtratorNfeCgu().executar_ingestao()
