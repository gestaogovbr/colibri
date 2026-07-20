import time
import logging
import requests
import utils.configurar_logging as log


class Downloader:
    """
    Classe responsável por baixar arquivos da internet com tratamento de erros e tentativas
    """
    def __init__(self):
        self.MAX_TENTATIVAS = 3
        self.TIMEOUT_SEGUNDOS = 60
        self.PAUSA_BASE_SEGUNDOS = 5
        log.setup_logging()
        self.logger = logging.getLogger(__name__)
        self._session = self._criar_sessao()


    def _criar_sessao(self) -> requests.Session:
        """Cria uma sessão HTTP persistente pra todas as requests"""
        session = requests.Session()
        session.headers.update({"User-Agent": "colibri-ingestao/1.0"})
        return session


    def _get_url(self, session: requests.Session, url: str) -> bytes | None:
        """Baixa a URL com até MAX_TENTATIVAS tentativas e retorna o payload em BYTES"""
        for tentativa in range(1, self.MAX_TENTATIVAS + 1):
            try:
                resposta = session.get(url, timeout=self.TIMEOUT_SEGUNDOS)
                if resposta.status_code == 404:
                    self.logger.debug(f"404 — sem dados: {url}")
                    return None
                resposta.raise_for_status()
                return resposta.content
            except requests.exceptions.Timeout:
                self.logger.warning(f"Timeout (tentativa {tentativa}/{self.MAX_TENTATIVAS}): {url}")
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Erro de rede (tentativa {tentativa}/{self.MAX_TENTATIVAS}): {e}")
            if tentativa < self.MAX_TENTATIVAS:
                time.sleep(self.PAUSA_BASE_SEGUNDOS * tentativa)
        self.logger.error(f"Falha definitiva: {url}")
        return None
    

    def baixar_arquivo(self, url: str) -> bytes | None:
        """Baixa o arquivo da URL reaproveitando a sessão HTTP da instância"""
        return self._get_url(self._session, url)


    def obter_tipo_conteudo(self, url: str) -> str:
        """Faz um HEAD request e retorna o Content-Type da URL (string vazia se indisponível)"""
        try:
            resposta = self._session.head(url, allow_redirects=True, timeout=self.TIMEOUT_SEGUNDOS)
            return resposta.headers.get("Content-Type", "")
        except requests.exceptions.RequestException:
            return ""