from datetime import datetime

B3_BASE_URL = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"
B3_DOWNLOAD_URL = "https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/eyJsYW5ndWFnZSI6InB0LWJyIiwicGFnZU51bWJlciI6MSwicGFnZVNpemUiOjEyMCwiaW5kZXgiOiJJQk9WIiwiY2xhc3NpZmljYXRpb24iOiIiLCJzZWdtZW50IjoiMSJ9"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Referer': B3_BASE_URL
}

OUTPUT_FILE_FORMAT = "IBOVDia_{}.csv"

DOWNLOAD_DIR = "downloads"

EXPECTED_COLUMNS = ['segment', 'cod', 'asset', 'type', 'part', 'partAcum', 'theoricalQty']

FILE_ENCODING = 'cp1252'

CSV_SEPARATOR = ';'