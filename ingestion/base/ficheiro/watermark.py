import csv
import utils.configurar_logging as log

from datetime import datetime
from pathlib import Path
from typing import override
from ingestion.base.ficheiro.ficheiro import Ficheiro
from utils.constantes import RAIZ_PROJETO

log.setup_logging()

_CHAVE_UNICA = "ultimo_recurso"


class Watermark(Ficheiro):
    """
    Lê, escreve e sincroniza com o bucket o watermark de um extrator:
        .Uma única entrada, com o último recurso baixado ("tabela:identificador")
         e o "extraido_em". Cada chamada a `registrar_entrada` sobrescreve essa
         entrada, então ao final de uma execução só o último recurso prevalece;
        .As duas colunas de `colunas_ficheiro` são, nessa ordem, a que guarda o
         "tabela:identificador" e a que guarda o "extraido_em".
    """

    DIRETORIO_WATERMARKS = Path(RAIZ_PROJETO) / "dados" / "watermarks"
    DIRETORIO_ALTERACOES = Path(RAIZ_PROJETO) / "dados" / "alteracoes" / "watermarks"
    PREFIXO_BUCKET = "watermarks"

    @override
    def caminho_ficheiro(self) -> Path:
        return self.DIRETORIO_WATERMARKS / self.nome_ficheiro


    @override
    def caminho_alteracoes(self) -> Path:
        return self.DIRETORIO_ALTERACOES / self.nome_alteracoes


    @override
    def _chave_bucket(self) -> str:
        return f"{self.PREFIXO_BUCKET}/{self.nome_ficheiro}"


    @override
    def carregar(self) -> dict[str, dict]:
        """Lê a única entrada do watermark (o último recurso baixado)"""
        caminho = self.caminho_ficheiro()
        if not caminho.exists():
            return {}
        with open(caminho, newline="", encoding="utf-8") as f:
            linhas = list(csv.DictReader(f))
        return {_CHAVE_UNICA: linhas[0]} if linhas else {}


    @override
    def salvar(self, ficheiro: dict[str, dict]) -> None:
        """Salva a única entrada do watermark, sobrescrevendo o arquivo inteiro"""
        with open(self.caminho_ficheiro(), "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.colunas_ficheiro)
            writer.writeheader()
            if ficheiro:
                writer.writerow(next(iter(ficheiro.values())))


    @override
    def registrar_entrada(
        self, ficheiro: dict[str, dict], tabela: str, identificador: str, conteudo: bytes, **extra: str
    ) -> None:
        """Sobrescreve a única entrada do watermark com este recurso, descartando a anterior"""
        coluna_recurso, coluna_extraido_em = self.colunas_ficheiro[0], self.colunas_ficheiro[1]
        ficheiro.clear()
        ficheiro[_CHAVE_UNICA] = {
            coluna_recurso: f"{tabela}:{identificador}",
            coluna_extraido_em: datetime.now().isoformat(timespec="seconds"),
        }
