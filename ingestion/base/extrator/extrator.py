import io
import logging
import zipfile
import utils.configurar_logging as log
from abc import ABC, abstractmethod
from datetime import date
from ingestion.base.conversor import Conversor
from ingestion.base.downloader import Downloader
from ingestion.base.ficheiro.ficheiro import Ficheiro
from ingestion.base.tarefa import Tarefa
from utils.carregar_segredo import carregar_segredo
from utils.constantes import NOME_SEGREDO_DESENVOLVEDOR

log.setup_logging()


class Extrator(ABC):
    """
    Classe-mãe dos extratores de dados:
        .Download, conversão de formato e loop de ingestão;
        .Implementação da classe define como processar diferentes tipos de arquivos
         (zip, csv, json, xlsx) e como salvar o rastreio (manifesto ou watermark).
    """

    NOME_SEGREDO = NOME_SEGREDO_DESENVOLVEDOR
    PREFIXO_BUCKET = "raw"

    def __init__(self):
        # Composição
        self.downloader = Downloader()
        self.logger = logging.getLogger(self.__class__.__module__)

        # Declarações
        self.periodo_inicio: date = date.today()


    @property
    @abstractmethod
    def _ficheiro(self) -> Ficheiro:
        """Retorna o Manifesto ou Watermark desta subclasse"""
        raise NotImplementedError


    @abstractmethod
    def url_periodo(self, periodo: str) -> str:
        """Retorna a URL do arquivo a ser baixado para o período informado"""
        raise NotImplementedError


    @abstractmethod
    def chave_periodo(self, tabela: str, periodo: str) -> str:
        """Retorna a chave do objeto no bucket para o período informado"""
        raise NotImplementedError


    @abstractmethod
    def _processar_arquivo(
        self,
        tarefa: Tarefa,
        estado: dict[str, dict],
        bucket_nome: str,
        conteudo: bytes,
        conteudo_final: bytes,
    ) -> str:
        """Salva `conteudo_final` no bucket e registra `conteudo` no rastreio, conforme a estratégia da subclasse"""
        raise NotImplementedError


    @abstractmethod
    def gerar_tarefas(self, estado: dict[str, dict]) -> list[Tarefa]:
        """Retorna a lista de tarefas desta execução (subclasses sem dedup por hash ignoram `estado`)"""
        raise NotImplementedError

    # Download + conversão

    def _processar_zip(self, tarefa: Tarefa, estado: dict[str, dict], bucket_nome: str) -> str:
        """Baixa um ZIP, extrai `tarefa.nome_no_zip` e converte pra Parquet"""
        conteudo = self.downloader.baixar_arquivo(tarefa.url) # baixa zip uma única vez, mesmo que haja várias tabelas dentro

        # Para cada arquivo dentro do zip, converte e salva no bucket, registrando o hash no manifesto
        if conteudo is None:
            return "indisponivel"
        try:
            with zipfile.ZipFile(io.BytesIO(conteudo)) as z:
                nomes = [n for n in z.namelist() if not n.endswith("/")] # lista arquivos dentro do zip
                for nome in nomes:
                    conteudo_arquivo = z.read(nome).decode("latin-1").encode("utf-8")
                    conteudo_final = Conversor.csv_para_parquet(conteudo_arquivo)
                    status = self._processar_arquivo(tarefa, estado, bucket_nome, conteudo, conteudo_final)
                return status
        except (zipfile.BadZipFile, StopIteration) as e:
            self.logger.error(f"Não foi possível identificar as tabelas no ZIP: {e}")


    def _processar_json(self, tarefa: Tarefa, estado: dict[str, dict], bucket_nome: str) -> str:
        """Baixa um JSON e converte pra Parquet"""
        conteudo = self.downloader.baixar_arquivo(tarefa.url)
        if conteudo is None:
            return "indisponivel"
        conteudo_final = Conversor.json_para_parquet(conteudo)
        return self._processar_arquivo(tarefa, estado, bucket_nome, conteudo, conteudo_final)


    def _processar_csv(self, tarefa: Tarefa, estado: dict[str, dict], bucket_nome: str) -> str:
        """Baixa um CSV e converte pra Parquet"""
        conteudo = self.downloader.baixar_arquivo(tarefa.url)
        if conteudo is None:
            return "indisponivel"
        conteudo_final = Conversor.csv_para_parquet(conteudo)
        return self._processar_arquivo(tarefa, estado, bucket_nome, conteudo, conteudo_final)


    def _processar_xlsx(self, tarefa: Tarefa, estado: dict[str, dict], bucket_nome: str) -> str:
        """Baixa um XLSX e salva como veio, sem conversão"""
        conteudo = self.downloader.baixar_arquivo(tarefa.url)
        if conteudo is None:
            return "indisponivel"
        return self._processar_arquivo(tarefa, estado, bucket_nome, conteudo, conteudo)

    # Execução

    def executar_ingestao(self, bucket_nome: str | None = None) -> bool:
        """
        Método orquestrador final do extrator:
            .Baixa o rastreio (manifesto ou watermark), processa todas as
             tarefas e salva as alterações;
            .Retorna True se algum dado novo foi extraído.
        """
        ficheiro = self._ficheiro
        ficheiro.caminho_ficheiro().parent.mkdir(parents=True, exist_ok=True)
        ficheiro.caminho_alteracoes().parent.mkdir(parents=True, exist_ok=True)

        bucket_nome = bucket_nome or carregar_segredo(self.NOME_SEGREDO)["bucket_lake"]
        ficheiro.baixar(bucket_nome)
        estado = ficheiro.carregar()
        tarefas = self.gerar_tarefas(estado)
        contadores: dict[str, int] = {}
        alteracoes: list[tuple[str, ...]] = []
        modificado = False
        self.logger.info(
            f"Ingestão: {self.periodo_inicio} -> {date.today()} ({len(tarefas)} tarefa(s))"
        )

        try:
            for i, tarefa in enumerate(tarefas, start=1):
                tipo_conteudo = self.downloader.obter_tipo_conteudo(tarefa.url).lower()
                if "zip" in tipo_conteudo:
                    status = self._processar_zip(tarefa, estado, bucket_nome)
                elif "json" in tipo_conteudo:
                    status = self._processar_json(tarefa, estado, bucket_nome)
                elif "sheet" in tipo_conteudo:  # xlsx: application/vnd.openxmlformats-...spreadsheetml.sheet
                    status = self._processar_xlsx(tarefa, estado, bucket_nome)
                else:
                    status = self._processar_csv(tarefa, estado, bucket_nome)  # default
                contadores[status] = contadores.get(status, 0) + 1
                if status not in ("ignorado", "indisponivel"):
                    modificado = True
                    alteracoes.extend(tarefa.alteracoes)
                ficheiro.salvar_periodicamente(i, estado)
        finally:
            ficheiro.salvar(estado)
            ficheiro.salvar_alteracoes(alteracoes)
            self.logger.info(f"Ficheiro: {ficheiro.caminho_ficheiro()}")
            self.logger.info(
                f"Alterações: {ficheiro.caminho_alteracoes()} ({len(alteracoes)} arquivo(s))"
            )

        self.logger.info("  ".join(f"{k.capitalize()}: {v}" for k, v in contadores.items()))
        return modificado
