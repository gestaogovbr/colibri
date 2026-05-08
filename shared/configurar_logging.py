import logging

# Templates de mensagens de log
ARQUIVO_NAO_ENCONTRADO = "Arquivo %s não encontrado."
ARQUIVO_LIDO_COM_SUCESSO = "Arquivo %s lido com sucesso."
LENDO_ARQUIVO = "Lendo arquivo: %s"
FORMATO_INVALIDO = "Tipo de arquivo não suportado: %s"
REQUISICAO_EXPIRADA = "Requisição para %s expirou após %s segundos."
REQUISICAO_FALHOU = "Falha na requisição para %s: %s"
ERRO_HTTP = "Erro HTTP ao buscar %s: %s"
JSON_INVALIDO = "Resposta JSON inválida de %s"
JSON_SUCESSO = "JSON obtido com sucesso de %s"
SEGREDO_NAO_ENCONTRADO = "Segredo '%s' não encontrado no arquivo YAML."
SEGREDO_VAZIO = "O nome do segredo não pode ser vazio."
SEGREDO_CAMPO_AUSENTE = "Campo ausente no segredo '%s': %s"


def setup_logging():
    "Função para configurar o logging com um formato consistente"
    root_logger = logging.getLogger()
    
    # Evita adicionar handlers duplicados
    if root_logger.hasHandlers():
        return
    
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    file_handler = logging.FileHandler("app.log", encoding="utf-8")
    file_handler.setFormatter(formatter)

    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)
    root_logger.addHandler(file_handler)
