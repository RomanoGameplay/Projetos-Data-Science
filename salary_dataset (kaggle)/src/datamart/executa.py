from .datamart import Datamart
from pathlib import Path


def executa_datamart(entrada: Path, tabela: str) -> None:
    """
    :param entrada: Caminho do qual será usado para receber os dados datamart
    :param tabela: Nome do arquivo datamart a ser manipulado
    """
    datamart = Datamart(entrada, tabela)
    datamart.pipeline()
