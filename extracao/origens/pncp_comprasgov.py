"""
Script para baixar dados de compras do governo (PNCP) do repositório de dados abertos.

Baixa arquivos dos anos 2021 até o ano anterior ao corrente (2025 em 2026)
do servidor HTTPS: https://repositorio.dados.gov.br/seges/comprasgov/anual/
"""

import click
import logging
from datetime import datetime
from pathlib import Path
import requests

import utils.configurar_logging as log


log.setup_logging()
logger = logging.getLogger(__name__)


def validar_datas(data_inicio_str, data_fim_str):
    """Valida e converte as datas de string para datetime."""
    try:
        data_inicio = datetime.strptime(data_inicio_str, '%Y-%m-%d')
        data_fim = datetime.strptime(data_fim_str, '%Y-%m-%d')
        
        if data_inicio > data_fim:
            raise ValueError('Data de início não pode ser posterior à data final')
        
        return data_inicio, data_fim
    except ValueError as e:
        logger.error(f'Erro ao validar datas: {e}')
        raise


def obter_anos_para_baixar(data_inicio, data_fim):
    """
    Determina quais anos devem ter seus arquivos baixados.
    
    Retorna os anos de 2021 até o ano anterior ao ano corrente (2025 em 2026),
    considerando o intervalo de datas fornecido.
    """
    ano_corrente = datetime.now().year
    ano_anterior = ano_corrente - 1
    
    # Anos disponíveis começam em 2021 até o ano anterior
    anos_disponiveis = list(range(2021, ano_anterior + 1))
    
    # Filtrar pelos anos que caem no intervalo de datas
    ano_inicio = data_inicio.year
    ano_fim = data_fim.year
    
    anos_filtrados = [
        ano for ano in anos_disponiveis 
        if ano_inicio <= ano <= ano_fim
    ]
    
    logger.info(f'Anos disponíveis para download: 2021-{ano_anterior}')
    logger.info(f'Anos a serem baixados (filtrados por data): {anos_filtrados}')
    
    return anos_filtrados


def criar_conexao_https():
    """Cria uma sessão HTTPS com configurações padrão."""
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'
    })
    return session


def listar_arquivos_ano(session, ano):
    """Lista os arquivos disponíveis para um determinado ano via HTTPS."""
    url_base = 'https://repositorio.dados.gov.br/seges/comprasgov/anual/'
    url_ano = f'{url_base}{ano}/'
    
    try:
        logger.info(f'Listando arquivos de {ano} em {url_ano}')
        response = session.get(url_ano, timeout=10)
        response.raise_for_status()
        
        # Fazer parsing simples do HTML para extrair nomes de arquivos
        # Procura por padrões de links de arquivo
        from html.parser import HTMLParser
        
        class LinkParser(HTMLParser):
            def __init__(self):
                super().__init__()
                self.links = []
            
            def handle_starttag(self, tag, attrs):
                if tag == 'a':
                    for attr, value in attrs:
                        if attr == 'href' and value and not value.startswith('/') and value != '../':
                            self.links.append(value)
        
        parser = LinkParser()
        parser.feed(response.text)
        
        # Filtrar apenas arquivos (evitar diretórios)
        arquivos = [link for link in parser.links if link and '.' in link]
        
        logger.info(f'Encontrados {len(arquivos)} arquivo(s) em {ano}')
        return arquivos
    
    except requests.exceptions.RequestException as e:
        logger.warning(f'Erro ao listar arquivos de {ano}: {e}')
        return []


def baixar_arquivo(session, ano, arquivo, diretorio_saida):
    """Baixa um arquivo específico via HTTPS."""
    url_base = 'https://repositorio.dados.gov.br/seges/comprasgov/anual/'
    url_arquivo = f'{url_base}{ano}/{arquivo}'
    
    caminho_local = Path(diretorio_saida) / str(ano)
    caminho_local.mkdir(parents=True, exist_ok=True)
    
    caminho_arquivo = caminho_local / arquivo
    
    try:
        logger.info(f'Baixando {arquivo} de {ano}...')
        
        response = session.get(url_arquivo, timeout=30, stream=True)
        response.raise_for_status()
        
        tamanho_total = int(response.headers.get('content-length', 0))
        
        with open(caminho_arquivo, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        logger.info(f'Arquivo salvo em: {caminho_arquivo} ({tamanho_total / 1024 / 1024:.2f} MB)')
        return True
    
    except requests.exceptions.RequestException as e:
        logger.error(f'Erro ao baixar {arquivo}: {e}')
        if caminho_arquivo.exists():
            caminho_arquivo.unlink()
        return False


def executar_downloads(anos, session, diretorio_saida):
    """Executa o download de todos os arquivos para os anos especificados."""
    total_arquivos = 0
    arquivos_baixados = 0
    
    for ano in anos:
        logger.info(f'\n--- Processando ano {ano} ---')
        try:
            arquivos = listar_arquivos_ano(session, ano)
            
            if not arquivos:
                logger.warning(f'Nenhum arquivo encontrado para {ano}')
                continue
            
            logger.info(f'Encontrados {len(arquivos)} arquivo(s) em {ano}')
            
            for arquivo in arquivos:
                total_arquivos += 1
                if baixar_arquivo(session, ano, arquivo, diretorio_saida):
                    arquivos_baixados += 1
        
        except Exception as e:
            logger.error(f'Erro ao processar o ano {ano}: {e}')
            continue
    
    return total_arquivos, arquivos_baixados


def main(data_inicio, data_fim, diretorio_saida):
    """Função principal do script."""
    try:
        # Validar datas
        data_inicio_obj, data_fim_obj = validar_datas(data_inicio, data_fim)
        logger.info(f'Período de download: {data_inicio_obj.date()} a {data_fim_obj.date()}')
        
        # Obter anos para baixar
        anos = obter_anos_para_baixar(data_inicio_obj, data_fim_obj)
        
        if not anos:
            logger.warning('Nenhum ano disponível para o período especificado')
            return
        
        # Criar diretório de saída
        Path(diretorio_saida).mkdir(parents=True, exist_ok=True)
        logger.info(f'Diretório de saída: {diretorio_saida}')
        
        # Criar sessão HTTPS e fazer download
        session = criar_conexao_https()
        
        try:
            total, baixados = executar_downloads(
                anos, 
                session, 
                diretorio_saida
            )
            
            logger.info(f'\n--- Resumo ---')
            logger.info(f'Total de arquivos encontrados: {total}')
            logger.info(f'Arquivos baixados com sucesso: {baixados}')
            logger.info(f'Taxa de sucesso: {(baixados/total*100):.1f}%' if total > 0 else 'N/A')
        
        finally:
            session.close()
            logger.info('Conexão HTTPS encerrada')
    
    except Exception as e:
        logger.error(f'Erro fatal: {e}')
        raise


@click.command()
@click.option(
    '--data-inicio',
    type=str,
    default='2021-01-01',
    required=True,
    help='Data de início no formato YYYY-MM-DD'
)
@click.option(
    '--data-fim',
    type=str,
    required=True,
    help='Data final no formato YYYY-MM-DD'
)
@click.option(
    '--diretorio-saida',
    type=str,
    default='./dados/pncp_comprasgov',
    help='Diretório para salvar os arquivos baixados (padrão: ./dados/pncp_comprasgov)'
)
def cli(data_inicio, data_fim, diretorio_saida):
    """Baixar dados de compras do governo (PNCP)."""
    main(data_inicio, data_fim, diretorio_saida)


if __name__ == '__main__':
    cli()
