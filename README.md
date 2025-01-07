# Projeto ETL B3 - Composição do IBOVESPA

Este projeto implementa um pipeline ETL completo para processar dados da composição do índice IBOVESPA. O sistema captura dados diários da B3, processa-os e disponibiliza para análise.

## Arquitetura

1. **Extração (Scrapper)**
  - Script Python que baixa os dados da B3
  - Salva os dados brutos em formato Parquet no S3
  - Particionamento por data

2. **Transformação (AWS Glue)**
  - Job Glue acionado automaticamente via Lambda
  - Agregação dos dados por tipo de ação  
  - Cálculo de médias e totais
  - Renomeação de colunas para melhor semântica

3. **Carga (Parquet/Athena)**
  - Dados refinados salvos em formato Parquet
  - Particionamento por data e tipo de ação
  - Disponibilização via AWS Athena para consultas SQL

## Tecnologias Utilizadas

- Python (Scrapper)
- AWS S3 (Armazenamento)
- AWS Lambda (Trigger)
- AWS Glue (Processamento) 
- AWS Athena (Consultas)
- Matplotlib (Visualização)

## Visualização dos Dados

O projeto inclui um notebook Athena que permite:
- Consultas SQL nos dados processados
- Geração de gráficos de evolução temporal
- Análise da participação por tipo de ação

## Estrutura do Projeto

b3_scrapy/
├── src/
│   ├── scrapper/
│   │   ├── init.py
│   │   ├── constants.py
│   │   ├── lambda_function.py
│   │   └── scrapper.py
│   └── run.py
└── requirements.txt