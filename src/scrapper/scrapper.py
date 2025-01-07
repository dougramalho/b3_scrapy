import os
import requests
import pandas as pd
from datetime import datetime
import logging
from bs4 import BeautifulSoup
import json
from .constants import (
    B3_BASE_URL,
    B3_DOWNLOAD_URL,
    HEADERS,
    OUTPUT_FILE_FORMAT,
    DOWNLOAD_DIR,
    EXPECTED_COLUMNS,
    FILE_ENCODING,
    CSV_SEPARATOR
)

# Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class B3Scrapper:
    def __init__(self):
        """Inicializa o scrapper da B3."""
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        
        # Cria o diretório de downloads se não existir
        if not os.path.exists(DOWNLOAD_DIR):
            os.makedirs(DOWNLOAD_DIR)

    def download_daily_ibov(self) -> str:
        """
        Faz o download do arquivo diário do IBOV.
        
        Returns:
            str: Caminho do arquivo baixado
        """
        try:
            # Primeiro acessa a página principal para obter possíveis tokens/cookies
            logger.info("Acessando página principal da B3...")
            response = self.session.get(B3_BASE_URL)
            response.raise_for_status()

            # Faz o download do arquivo CSV
            logger.info("Fazendo download do arquivo CSV...")
            response = self.session.get(B3_DOWNLOAD_URL)
            response.raise_for_status()

            # Gera o nome do arquivo com a data atual
            current_date = datetime.now().strftime('%d%m%y')
            filename = OUTPUT_FILE_FORMAT.format(current_date)
            filepath = os.path.join(DOWNLOAD_DIR, filename)

            # Salva o arquivo
            with open(filepath, 'wb') as f:
                f.write(response.content)

            logger.info(f"Arquivo baixado com sucesso: {filepath}")
            
            # Log do conteúdo do arquivo para debug
            with open(filepath, 'r', encoding=FILE_ENCODING) as f:
                content = f.read()
                logger.info(f"Conteúdo do arquivo:\n{content[:500]}...")
            
            # Verifica se o conteúdo é JSON
            try:
                json_content = json.loads(response.content)
                logger.info(f"Conteúdo é JSON: {json_content}")
                
                # Se for JSON, precisamos extrair os dados e converter para CSV
                if 'results' in json_content:
                    logger.info("Convertendo JSON para CSV...")
                    df = pd.DataFrame(json_content['results'])
                    # Salva como CSV
                    df.to_csv(filepath, sep=CSV_SEPARATOR, encoding=FILE_ENCODING, index=False)
            except json.JSONDecodeError:
                logger.info("Conteúdo não é JSON, assumindo CSV...")
            
            # Valida o arquivo baixado
            self._validate_file(filepath)
            
            return filepath

        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao baixar arquivo: {str(e)}")
            raise

    def _validate_file(self, filepath: str):
        """
        Valida se o arquivo baixado está no formato esperado.
        
        Args:
            filepath (str): Caminho do arquivo a ser validado
        """
        try:
            # Lê o arquivo primeiro como texto para verificar o conteúdo
            with open(filepath, 'r', encoding=FILE_ENCODING) as f:
                lines = f.readlines()
                logger.info(f"Número de linhas no arquivo: {len(lines)}")
                if len(lines) > 0:
                    logger.info(f"Primeira linha: {lines[0]}")
                if len(lines) > 1:
                    logger.info(f"Segunda linha: {lines[1]}")

            # Tenta diferentes abordagens de leitura
            try:
                # Tenta ler sem pular linhas primeiro
                df = pd.read_csv(filepath, encoding=FILE_ENCODING, sep=CSV_SEPARATOR)
            except:
                logger.info("Tentando ler pulando a primeira linha...")
                df = pd.read_csv(filepath, encoding=FILE_ENCODING, sep=CSV_SEPARATOR, skiprows=1)

            logger.info(f"Colunas encontradas: {df.columns.tolist()}")
            logger.info(f"Dimensões do DataFrame: {df.shape}")

            # Verifica se tem dados
            if df.empty:
                raise ValueError("DataFrame está vazio")

            # Verifica se todas as colunas esperadas estão presentes
            missing_columns = set(EXPECTED_COLUMNS) - set(df.columns)
            if missing_columns:
                raise ValueError(f"Colunas ausentes no arquivo: {missing_columns}")

            logger.info("Arquivo validado com sucesso")

        except Exception as e:
            logger.error(f"Erro ao validar arquivo: {str(e)}")
            raise

    def process_file(self, filepath: str) -> pd.DataFrame:
        """
        Processa o arquivo baixado e retorna um DataFrame.
        
        Args:
            filepath (str): Caminho do arquivo a ser processado
            
        Returns:
            pd.DataFrame: DataFrame com os dados processados
        """
        try:
            df = pd.read_csv(
                filepath,
                encoding=FILE_ENCODING,
                sep=CSV_SEPARATOR
            )

            df = df.apply(lambda x: x.str.strip() if isinstance(x, pd.Series) and x.dtype == "object" else x)
            
            df['part'] = df['part'].str.replace(',', '.').astype(float)
            
            df['theoricalQty'] = df['theoricalQty'].str.replace('.', '').astype(float)
            
            df['data_referencia'] = datetime.now().date()

            logger.info(f"Processamento concluído. Shape do DataFrame: {df.shape}")
            return df

        except Exception as e:
            logger.error(f"Erro ao processar arquivo: {str(e)}")
            raise