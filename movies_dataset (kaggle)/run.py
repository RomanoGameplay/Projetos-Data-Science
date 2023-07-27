import click as cli


@cli.group()
def grupo_principal():
    """
    Grupo principal de comandos
    """
    pass


@grupo_principal.group()
def aquisicao():
    """
    Grupo de comando destinado ao tratamento dos dados
    """
    pass


@aquisicao.command()
@cli.option()
def processa_dado():
    pass


if __name__ == '__main__':
    grupo_principal()
