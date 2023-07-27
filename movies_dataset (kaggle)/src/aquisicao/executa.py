from src.aquisicao.opcoes import *
from pathlib import Path


def executa_etl(etl: str, entrada: Path, saida: Path, criar_caminho: bool, reprocessar: bool) -> None:
    """
    Executa o ETL dos dados
    :param etl: Nome do ETL a ser executado
    :param entrada: Caminho de entrada dos dados
    :param saida: Caminho de saída dos dados
    :param criar_caminho: Flag indicando necessidade de criar caminho
    :param reprocessar: Flag indicando necessidade de reprocessamento dos dados
    """
    obj = ETL_DICT[EnumMovies(etl)](entrada, saida, criar_caminho, reprocessar)
    obj.pipeline()
