import logging
from scrapper.scrapper import B3Scrapper
from scrapper.s3_manager import S3Manager
from datetime import datetime
from dotenv import load_dotenv


load_dotenv()

# Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run():
    """
    Função principal que executa o scrapper.
    """
    try:
        scrapper = B3Scrapper()
        s3_manager = S3Manager()
        
        filepath = scrapper.download_daily_ibov()
        
        df = scrapper.process_file(filepath)
        
        logger.info(f"Dados processados com sucesso. Shape do DataFrame: {df.shape}")
        
        reference_date = datetime.now()
        s3_uri = s3_manager.upload_to_s3(df, reference_date)
        logger.info(f"Processamento concluído com sucesso. Arquivo salvo em: {s3_uri}")
        
    except Exception as e:
        logger.error(f"Erro ao executar o scrapper: {str(e)}")
        raise

if __name__ == "__main__":
    run()