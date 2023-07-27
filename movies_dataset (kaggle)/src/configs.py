import os

PROJECT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PASTA_DADOS = f'{PROJECT_PATH}/dados'
CAMINHO_ENTRADA = f'{PASTA_DADOS}/entrada'
CAMINHO_SAIDA = f'{PASTA_DADOS}/saida'

print(PROJECT_PATH)
print(PASTA_DADOS)
print(CAMINHO_ENTRADA)
print(CAMINHO_SAIDA)
