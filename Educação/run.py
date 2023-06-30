import click
from src.aquisicao.opcoes import EnumETL, ETL_DICT
from src.config import *


@click.group()
def cli():
    pass


@cli.group()
def aquisicao():
    """
    Grupo de comandos que executam as funções de aquisição
    """


@aquisicao.command()
@click.option('--etl', type=click.Choice([s.value for s in EnumETL]), help='Nome do ETL a ser executado')
@click.option('--entrada', default=PASTA_DADOS, help='String com o caminho para a pasta de entrada')
@click.option('--saida', default=PASTA_SAIDA_AQUISICAO, help='String com o caminho para a pasta de saida')
@click.option('--criar_caminho', default=True, help='Flag indicando se devemos criar os caminhos')
def processa_dado(
        etl: str,
        entrada: str,
        saida: str,
        criar_caminho: bool
) -> None:
    """
    Executa o pipeline de ETL de uma determinada fonte

    :param etl: nome do ETL a ser realizado
    :param entrada: string com o caminho para a pasta de entrada
    :param saida: string com o caminho para a pasta de saida
    :param criar_caminho: flag indicando se devemos criar os caminhos
    """
    objeto = ETL_DICT[EnumETL(etl)](entrada, saida, criar_caminho)
    objeto.pipeline()


if __name__ == '__main__':
    cli()
