import click
from src.utils.logs import configura_logs
from src.aquisicao.opcoes import EnumETL, ETL_DICT
from src.utils.configs import *


@click.group()
def comm_principal():
    pass


@comm_principal.group()
def aquisicao():
    """
    Grupo de comandos que executam as funções de aquisição
    """
    pass


@aquisicao.command()
@click.option('--etl', '-etl', 'etl', type=click.Choice([s.value for s in EnumETL]), help='Nome do ETL a ser executado')
@click.option('--entrada', '-e', 'entrada', default=CAMINHO_ENTRADA,
              help='String com o caminho para a pasta de entrada')
@click.option('--saida', '-s', 'saida', default=CAMINHO_SAIDA, help='String indicando o caminho para a pasta de saida')
@click.option('--criar_caminho', '-cc', 'criar_caminho', default='', help='Flag indicando necessidade de criar caminho')
def processa_dado(etl: str, entrada: str, saida: str, criar_caminho: bool) -> None:
    """
    Executa o pipeline de uma determinada fonte
    :param etl: Nome do ETL a ser executado
    :param entrada: String com o caminho para a pasta de entrada
    :param saida: String indicando o caminho para a pasta de saida
    :param criar_caminho: Flag indicando necessidade de criar caminho
    """
    configura_logs()
    obj = ETL_DICT[EnumETL(etl)](entrada, saida, criar_caminho)
    obj.pipeline()


if __name__ == '__main__':
    comm_principal()
