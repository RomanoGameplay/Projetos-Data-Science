from pathlib import Path

import click
from src.aquisicao.opcoes import EnumMovies, ETL_DICT
from src.configs import CAMINHO_ENTRADA, CAMINHO_SAIDA
from src.utils.logs import configura_logs
from src.aquisicao.executa import executa_etl


@click.group()
def grupo_principal() -> None:
    """
    Grupo principal de comandos
    """
    pass


@grupo_principal.group()
def aquisicao() -> None:
    """
    Grupo de comando destinado ao tratamento dos dados
    """
    pass


@aquisicao.command()
@click.option('--etl', '-etl', 'etl', default='MOVIES', type=click.Choice([s.value for s in EnumMovies]),
              help='Nome do ETL a ser executado')
@click.option('--entrada', '-e', 'entrada', default=CAMINHO_ENTRADA, help='Caminho de entrada os dados',
              type=click.Path(resolve_path=True, path_type=Path, file_okay=False))
@click.option('--saida', '-s', 'saida', default=CAMINHO_SAIDA, help='Caminho de saida para os dados',
              type=click.Path(resolve_path=True, path_type=Path, file_okay=False))
@click.option('--nao-criar_caminho', is_flag=True, show_default=True,
              help='Flag indicando necessidade de criar o caminho')
@click.option('--nao-reprocessar', is_flag=True, show_default=True,
              help='Flag indicando necessidade de reprocessar os dados')
def processa_dado(etl: str, entrada: Path, saida: Path, nao_criar_caminho: bool, nao_reprocessar: bool) -> None:
    """
    Comando responsável por executar o pipeline dos dados
    :param etl: Nome do ETL a ser executado
    :param entrada: Caminho de entrada dos dados
    :param saida: Caminho de saída para os dados
    :param nao_criar_caminho: Flag indicando necessidade de criar caminho para os dados
    :param nao_reprocessar: Flag indicando necessidade de reprocessamento dos dados
    """
    configura_logs()
    executa_etl(
        etl=etl,
        entrada=entrada,
        saida=saida,
        criar_caminho=not nao_criar_caminho,
        reprocessar=not nao_reprocessar
    )


if __name__ == '__main__':
    grupo_principal()
