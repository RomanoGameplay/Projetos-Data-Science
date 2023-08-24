import click as cli


@cli.group
def grupo_principal():
    pass


@grupo_principal.group
def aquisicao():
    """
    Grupo de comandos que executam as funções de aquisição
    """
    pass


@aquisicao.command()
def processa_dado():
    pass


if __name__ == '__main__':
    grupo_principal()
