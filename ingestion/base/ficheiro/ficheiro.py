import csv
import logging
import utils.configurar_logging as log
from abc import ABC, abstractmethod
from pathlib import Path
from utils.baixar_arquivo import baixar_arquivo_do_bucket
from utils.salvar_arquivo_no_bucket import salvar_arquivo_no_bucket

log.setup_logging()


class Ficheiro(ABC):
    """
    Lê, escreve e sincroniza com o bucket o ficheiro e o registro de alterações de um extrator
    """

    SALVAR_A_CADA = 50

    def __init__(
        self,
        nome_ficheiro: str,
        colunas_ficheiro: list[str],
        nome_alteracoes: str,
        colunas_alteracoes: list[str],
        nome_segredo: str,
        logger: logging.Logger,
    ):
        self.nome_ficheiro = nome_ficheiro
        self.colunas_ficheiro = colunas_ficheiro
        self.nome_alteracoes = nome_alteracoes
        self.colunas_alteracoes = colunas_alteracoes
        self.nome_segredo = nome_segredo
        self.logger = logger


    @abstractmethod
    def caminho_ficheiro(self) -> Path:
        raise NotImplementedError


    @abstractmethod
    def caminho_alteracoes(self) -> Path:
        raise NotImplementedError


    @abstractmethod
    def _chave_bucket(self) -> str:
        raise NotImplementedError


    @abstractmethod
    def registrar_entrada(
        self, ficheiro: dict[str, dict], tabela: str, periodo: str, conteudo: bytes, **extra: str
    ) -> None:
        raise NotImplementedError


    def baixar(self, bucket_nome: str) -> None:
        """Baixa o ficheiro do bucket pro diretório local"""
        self.caminho_ficheiro().unlink(missing_ok=True)
        self.logger.info(f"baixando ficheiro do bucket: {bucket_nome}/{self._chave_bucket()}")
        try:
            baixar_arquivo_do_bucket(
                self._chave_bucket(), bucket_nome, self.nome_segredo, str(self.caminho_ficheiro())
            )
            self.logger.info(f"ficheiro baixado do bucket: {bucket_nome}/{self._chave_bucket()}")
        except Exception as e:
            self.logger.warning(f"ficheiro não encontrado no bucket, iniciando do zero: {e}")


    def carregar(self) -> dict[str, dict]:
        """Lê o ficheiro CSV e retorna o dicionário indexado por "tabela:periodo" """
        caminho = self.caminho_ficheiro()
        if not caminho.exists():
            return {}
        tabela, periodo = self.colunas_ficheiro[0], self.colunas_ficheiro[1]
        ficheiro: dict[str, dict] = {}
        with open(caminho, newline="", encoding="utf-8") as f:
            for linha in csv.DictReader(f):
                ficheiro[f"{linha[tabela]}:{linha[periodo]}"] = linha
        return ficheiro


    def salvar(self, ficheiro: dict[str, dict]) -> None:
        """Salva o ficheiro em CSV, ordenado pelas duas colunas-chave"""
        tabela, periodo = self.colunas_ficheiro[0], self.colunas_ficheiro[1]
        with open(self.caminho_ficheiro(), "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.colunas_ficheiro)
            writer.writeheader()
            writer.writerows(
                sorted(ficheiro.values(), key=lambda e: (e[tabela], e[periodo]))
            )


    def salvar_periodicamente(self, indice: int, ficheiro: dict[str, dict]) -> None:
        """Salva localmente a cada `SALVAR_A_CADA` chamadas, pra não perder progresso em execuções longas"""
        if indice % self.SALVAR_A_CADA == 0:
            self.salvar(ficheiro)


    def salvar_alteracoes(self, alteracoes: list[tuple[str, ...]]) -> None:
        """Salva a lista de alterações desta execução, já no formato de `colunas_alteracoes`"""
        with open(self.caminho_alteracoes(), "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(self.colunas_alteracoes)
            writer.writerows(alteracoes)


    def subir(self, bucket_nome: str) -> None:
        """Sobe o ficheiro local pro bucket. Só deve ser chamado depois do dbt rodar com sucesso"""
        if not self.caminho_ficheiro().exists():
            return
        try:
            salvar_arquivo_no_bucket(self.caminho_ficheiro(), bucket_nome, self.nome_segredo, self._chave_bucket())
        except Exception as e:
            self.logger.warning(f"Não foi possível salvar ficheiro no bucket: {e}")


    def resetar(self) -> None:
        """Apaga o ficheiro e as alterações locais (usado por --do-zero)"""
        self.caminho_ficheiro().unlink(missing_ok=True)
        self.caminho_alteracoes().unlink(missing_ok=True)
