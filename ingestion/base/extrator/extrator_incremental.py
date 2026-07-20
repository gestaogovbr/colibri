import logging
import utils.configurar_logging as log
from ingestion.base.extrator.extrator import Extrator
from ingestion.base.ficheiro.ficheiro import Ficheiro
from ingestion.base.ficheiro.watermark import Watermark
from ingestion.base.tarefa import Tarefa
from utils.salvar_bytes_no_bucket import salvar_bytes_no_bucket

log.setup_logging()


class ExtratorIncremental(Extrator):
    """
    Extrator sem dedup:
        .Implementação envolve somente *sobrescrever* `_processar_arquivo`, usando o watermark;
        .Usa um Watermark a fim de registrar o último recurso baixado.
    """

    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(self.__class__.__module__)
        self.watermark: Watermark


    @property
    def _ficheiro(self) -> Ficheiro:
        return self.watermark


    # Processamento

    def _processar_arquivo(
        self,
        tarefa: Tarefa,
        estado: dict[str, dict],
        bucket_nome: str,
        conteudo: bytes,
        conteudo_final: bytes,
    ) -> str:
        """
        Salva `conteudo_final` no bucket e sobrescreve o watermark com este recurso:
            .`gerar_tarefas` já filtrou o que não precisa ser buscado, então
             toda tarefa que chega aqui é nova;
            .Retorna "baixado".
        """
        chave = f"{tarefa.tabela}:{tarefa.identificador}" # ex: VW_FT_PNCP_COMPRA:2021-12-01
        nome_no_bucket = f"{self.PREFIXO_BUCKET}/{tarefa.chave_bucket.as_posix()}"

        salvar_bytes_no_bucket(conteudo_final, bucket_nome, self.NOME_SEGREDO, nome_no_bucket)
        self.watermark.registrar_entrada(estado, tarefa.tabela, tarefa.identificador, conteudo, url=tarefa.url)
        self.logger.info(f"[{chave}]: baixado ({len(conteudo_final) / 1_048_576:.2f} MB)")
        return "baixado"
