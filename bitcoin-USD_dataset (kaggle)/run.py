from pathlib import Path

import click as cli

from src.configs import PATH_ENTRADA, PATH_SAIDA
from src.utils.logs import configura_logs
from src.aquisicao.executa import executa_etl


@cli.group
def grupo_principal() -> None:
    pass


@grupo_principal.group
def aquisicao() -> None:
    """
    Grupo de comandos que executam as funções de aquisição
    """
    pass


@aquisicao.command()
@cli.option('--e', '-e', 'entrada', default=PATH_ENTRADA, help='Caminho de entrada dos dados',
            type=cli.Path(resolve_path=True, path_type=Path, file_okay=False))
@cli.option('--s', '-s', 'saida', default=PATH_SAIDA, help='Caminho de saida dos dados',
            type=cli.Path(resolve_path=True, path_type=Path, file_okay=False))
@cli.option('--nao_criar_caminho', is_flag=True, show_default=True,
            help='Flag que indica necessidade de criar caminho dos dados')
def processa_dado(entrada: Path, saida: Path, nao_criar_caminho: bool) -> None:
    """
    Comando responsável por executar processamento dos dados
    :param entrada: Caminho de entrada dos dados
    :param saida: Caminho de saida dos dados
    :param nao_criar_caminho: Flag que indica necessidade de criar caminho dos dados
    """
    configura_logs()
    executa_etl(
        entrada=entrada,
        saida=saida,
        criar_caminho=not nao_criar_caminho
    )


if __name__ == '__main__':
    grupo_principal()
