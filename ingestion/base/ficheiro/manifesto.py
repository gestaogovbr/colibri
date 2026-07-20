import csv
import hashlib
import io
import utils.configurar_logging as log

from datetime import datetime
from pathlib import Path
from typing import override
from ingestion.base.ficheiro.ficheiro import Ficheiro
from utils.constantes import RAIZ_PROJETO

log.setup_logging()


class Manifesto(Ficheiro):
    """
    As duas primeiras colunas de `colunas_ficheiro` formam a chave composta
    de cada entrada (ex: "view"/"data" no PNCP, "tabela"/"periodo" no NFe-CGU)
    """

    DIRETORIO_FICHEIROS = Path(RAIZ_PROJETO) / "dados" / "manifestos"
    DIRETORIO_ALTERACOES = Path(RAIZ_PROJETO) / "dados" / "alteracoes" / "manifestos"
    PREFIXO_BUCKET = "ficheiros/manifestos"

    @override
    def caminho_ficheiro(self) -> Path:
        return self.DIRETORIO_FICHEIROS / self.nome_ficheiro

    
    @override
    def caminho_alteracoes(self) -> Path:
        return self.DIRETORIO_ALTERACOES / self.nome_alteracoes


    @override
    def _chave_bucket(self) -> str:
        return f"{self.PREFIXO_BUCKET}/{self.nome_ficheiro}"


    @override
    def registrar_entrada(
        self, ficheiro: dict[str, dict], tabela: str, identificador: str, conteudo: bytes, **extra: str
    ) -> None:
        """
        Registra a entrada "tabela:identificador" no manifesto:
            .Calcula os campos deriváveis de `conteudo` (hash, linhas, colunas,
             tamanho, timestamp) e mantém só os que a fonte declarou em `colunas_ficheiro`.
        """
        coluna_tabela, coluna_periodo = self.colunas_ficheiro[0], self.colunas_ficheiro[1]
        try:
            linhas = list(csv.reader(io.StringIO(conteudo.decode("utf-8"))))
            num_linhas, num_colunas = len(linhas) - 1, len(linhas[0])
        except (UnicodeDecodeError, IndexError):
            # Conteúdo não é CSV texto (ex: ZIP, XLSX) - sem linhas/colunas pra contar
            num_linhas = num_colunas = None
        campos = {
            coluna_tabela: tabela,
            coluna_periodo: identificador,
            "hash_sha256": hashlib.sha256(conteudo).hexdigest(),
            "num_linhas": num_linhas,
            "num_colunas": num_colunas,
            "tamanho_bytes": len(conteudo),
            "extraido_em": datetime.now().isoformat(timespec="seconds"),
            **extra,
        }
        ficheiro[f"{tabela}:{identificador}"] = {
            campo: valor for campo, valor in campos.items() if campo in self.colunas_ficheiro
        }