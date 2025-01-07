import json
import boto3
import os
import logging
from urllib.parse import unquote

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        logger.info("Evento recebido:")
        logger.info(json.dumps(event))
        
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        
        logger.info(f"Bucket: {bucket}")
        logger.info(f"Key: {key}")
        
        file_path = f's3://{bucket}/{key}'
        file_path = unquote(file_path)
        
        logger.info(f"Caminho completo do arquivo: {file_path}")
        
        glue_client = boto3.client('glue')
        
        job_args = {
            '--input_file': file_path,
            '--JOB_NAME': 'glue-etl-bovespa',
            '--enable-continuous-cloudwatch-log': 'true'
        }
        
        logger.info("Iniciando job do Glue com argumentos:")
        logger.info(json.dumps(job_args))
        
        response = glue_client.start_job_run(
            JobName='glue-etl-bovespa-v2',
            Arguments=job_args
        )
        
        logger.info("Resposta do Glue:")
        logger.info(json.dumps(response))
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Glue job iniciado com sucesso',
                'jobRunId': response['JobRunId'],
                'processedFile': file_path
            })
        }
        
    except Exception as e:
        logger.error(f"Erro na execução da Lambda: {str(e)}", exc_info=True)
        raise {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Erro ao processar arquivo'
            })
        }