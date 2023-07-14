import yaml
import typing
from pathlib import Path


CAMINHO_INFO = Path(__file__).parent.parent / 'info'


def carrega_yaml(nome_yaml: str) -> typing.Dict[str, typing.Any]:
    """
    Carrega arquivo yaml da pasta "info" da ferramenta

    :param nome_yaml: Nome do aqruivo yaml a ser carregado
    :return: dicionário com conteúdo do arquivo
    """
    global CAMINHO_INFO
    with open(CAMINHO_INFO / nome_yaml, 'r') as f:
        return yaml.load(f, Loader=yaml.FullLoader)
