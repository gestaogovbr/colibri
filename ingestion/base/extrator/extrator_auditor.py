import hashlib
import logging
import boto3
import botocore
import utils.configurar_logging as log
from ingestion.base.extrator.extrator import Extrator
from ingestion.base.ficheiro.ficheiro import Ficheiro
from ingestion.base.ficheiro.manifesto import Manifesto
from ingestion.base.tarefa import Tarefa
from utils.carregar_segredo import carregar_segredo
from utils.salvar_bytes_no_bucket import salvar_bytes_no_bucket

log.setup_logging()


class ExtratorAuditor(Extrator):
    """
    Extrator com dedup por hash:
        .Implementação envolve somente *sobrescrever* `_processar_arquivo`, usando o manifesto;
        .Usa um Manifesto para registrar o hash de cada arquivo já processado e pular o que não
         mudou desde a última execução.
    """

    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(self.__class__.__module__)
        self.manifesto: Manifesto


    @property
    def _ficheiro(self) -> Ficheiro:
        return self.manifesto


    # Processamento

    def _existe_e_bate_com_manifesto(
        self, entrada: dict | None, conteudo: bytes, bucket_nome: str, nome_no_bucket: str
    ) -> bool:
        """True se o hash do conteúdo bate com o manifesto e o objeto já existe no bucket"""
        if not entrada or entrada["hash_sha256"] != hashlib.sha256(conteudo).hexdigest():
            return False
        config = carregar_segredo(self.NOME_SEGREDO)
        s3 = boto3.resource(
            "s3",
            endpoint_url=config["endpoint"],
            aws_access_key_id=config["access_key"],
            aws_secret_access_key=config["secret_key"],
            region_name="auto",
        )
        try:
            s3.Object(bucket_nome, nome_no_bucket).load()
            return True
        except botocore.exceptions.ClientError:
            return False


    def _processar_arquivo(
        self,
        tarefa: Tarefa,
        estado: dict[str, dict],
        bucket_nome: str,
        conteudo: bytes,
        conteudo_final: bytes,
    ) -> str:
        """
        Salva `conteudo_final` no bucket e registra `conteudo` no manifesto, caso necessário:
            .Retorna "baixado", "atualizado" ou "ignorado".
        """
        chave = f"{tarefa.tabela}:{tarefa.identificador}" # ex: VW_FT_PNCP_COMPRA:2021-12-01
        entrada = estado.get(chave)
        nome_no_bucket = f"{self.PREFIXO_BUCKET}/{tarefa.chave_bucket.as_posix()}"

        # Caso zip, permite sobrescrita de entrada no manifesto (evita pular a partir do segundo elemento do zip)
        tipo_conteudo = self.downloader.obter_tipo_conteudo(tarefa.url).lower()
        is_zip = tipo_conteudo == "application/x-zip-compressed"

        # Não pula se for zip
        if self._existe_e_bate_com_manifesto(entrada, conteudo, bucket_nome, nome_no_bucket) and not is_zip:
            self.logger.info(f"[{chave}]: hash bate com manifesto, pulando")
            return "ignorado"

        salvar_bytes_no_bucket(conteudo_final, bucket_nome, self.NOME_SEGREDO, nome_no_bucket)
        self.manifesto.registrar_entrada(estado, tarefa.tabela, tarefa.identificador, conteudo, url=tarefa.url)
        status = "atualizado" if entrada else "baixado"
        e = estado[chave]
        self.logger.info(f"[{chave}]: {status} ({int(e['tamanho_bytes']) / 1_048_576:.2f} MB, {e['num_linhas']} linhas)")
        return status
