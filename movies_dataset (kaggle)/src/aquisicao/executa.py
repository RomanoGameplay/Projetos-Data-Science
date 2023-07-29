from src.aquisicao.movies import MoviesETL
from pathlib import Path
from src.aquisicao.opcoes import *


def executa_etl(etl: str, entrada: Path, saida: Path, criar_caminho: bool, reprocessar: bool) -> None:
    """
    Função que executa o ETL
    :param etl: Nome do ETL a ser executado
    :param entrada: Caminho de entrada dos dados
    :param saida: Caminho de saída para os dados
    :param criar_caminho: Flag indicando necessidade de criação de um caminho
    :param reprocessar: Flag indicando necessidade de reprocessamento dos dados
    """
    obj = ETL_DICT[EnumMovies(etl)](entrada, saida, criar_caminho, reprocessar)
    obj.pipeline()
