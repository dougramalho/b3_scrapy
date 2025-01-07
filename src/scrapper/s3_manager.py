import os
import boto3
import logging
from datetime import datetime
import pandas as pd

logger = logging.getLogger(__name__)

class S3Manager:
    def __init__(self):
        """Inicializa o gerenciador do S3."""
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            aws_session_token=os.getenv('AWS_SESSION_TOKEN'),
            region_name=os.getenv('AWS_REGION', 'us-east-1')
        )
        self.bucket_name = 'raw-b3'
        self.prefix = 'raw'

    def upload_to_s3(self, df: pd.DataFrame, partition_date: datetime) -> str:
        """
        Salva o DataFrame em formato parquet e faz upload para o S3 com particionamento por data.
        
        Args:
            df (pd.DataFrame): DataFrame a ser salvo
            partition_date (datetime): Data para particionamento
            
        Returns:
            str: S3 URI do arquivo salvo
        """
        try:
            temp_dir = "temp_parquet"
            if not os.path.exists(temp_dir):
                os.makedirs(temp_dir)

            s3_key = f"{self.prefix}/year={partition_date.year}/month={partition_date.month:02d}/day={partition_date.day:02d}/bovespa_data.parquet"

            source_path = f"s3://{self.bucket_name}/{s3_key}"
            df['source_path'] = source_path 

            local_file = os.path.join(temp_dir, f"bovespa_data_{partition_date.strftime('%Y%m%d')}.parquet")
            
            logger.info(f"Salvando arquivo parquet localmente: {local_file}")

            df.to_parquet(local_file, index=False)

            logger.info(f"Fazendo upload para o S3: s3://{self.bucket_name}/{s3_key}")
            self.s3_client.upload_file(local_file, self.bucket_name, s3_key)
            
            os.remove(local_file)
            
            return f"s3://{self.bucket_name}/{s3_key}"

        except Exception as e:
            logger.error(f"Erro ao fazer upload para o S3: {str(e)}")
            raise

    def check_if_exists(self, partition_date: datetime) -> bool:
        """
        Verifica se já existe um arquivo para a data especificada.
        
        Args:
            partition_date (datetime): Data para verificar
            
        Returns:
            bool: True se existe, False caso contrário
        """
        try:
            s3_key = f"{self.prefix}/year={partition_date.year}/month={partition_date.month:02d}/day={partition_date.day:02d}/bovespa_data.parquet"
            
            try:
                self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
                return True
            except self.s3_client.exceptions.ClientError as e:
                if e.response['Error']['Code'] == '404':
                    return False
                raise

        except Exception as e:
            logger.error(f"Erro ao verificar arquivo no S3: {str(e)}")
            raise