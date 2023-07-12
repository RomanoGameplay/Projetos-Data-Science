from pathlib import Path

import click

from src.utils.const import *
from src.aquisicao.executa import executa_etl
from src.utils.logs import configura_logs


@click.group()
def grupo_principal():
    """
    Grupo principal responsável por agrupar comandos para serem executados pelo terminal
    """
    pass


@grupo_principal.group()
def aquisicao() -> None:
    pass


@aquisicao.command()
@click.argument('data', default='Salary_Data.csv')
@click.option('--aq', '-aq', 'entrada', default=CAMINHO_ENTRADA,
              type=click.Path(path_type=Path, resolve_path=True, file_okay=False),
              help='Indica o caminho de entrada dos dados')
@click.option('--ext', '-ext', 'saida', default=CAMINHO_SAIDA,
              type=click.Path(path_type=Path, resolve_path=True, file_okay=False),
              help='Indica o caminho de saída dos dados')
@click.option("--criar-caminho", '-cc', 'criar_caminho', is_flag=True, show_default=True,
              help='Flag indicando necessidade de criar os caminhos')
@click.option('--re', '-re', 'reprocessar', is_flag=True, show_default=True,
              help='Flag indicando necessidade de reprocessar os dados')
def processa_dados(data: str, entrada: Path, saida: Path, criar_caminho: bool = True, reprocessar: bool = False):
    """
    Comando responsável por realizar o processamento dos dados

    :param data: Nomes dos dados a serem manipulados
    :param entrada: Caminho de entrada até os dados alvo
    :param saida: Caminho de saida até os dados alvo
    :param criar_caminho: flag indicando necessidade de criar caminho
    :param reprocessar: flag indicando necessidade de reprocessamento dos dados
    """
    configura_logs()
    executa_etl(
        dataset=data,
        entrada=entrada,
        saida=saida,
        criar_caminho=criar_caminho,
        reprocessar=reprocessar
    )


if __name__ == '__main__':
    grupo_principal()
