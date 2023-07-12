from pathlib import Path
from .salary import GetData


def executa_etl(dataset: str, entrada: Path, saida: Path, criar_caminho: bool, reprocessar: bool) -> None:
    """
    Executa o pipeline dos dados

    :param dataset: Conjunto de dados
    :param entrada: caminho indicando a origem dos dados
    :param saida: caimnho indicando a saida dos dados
    :param criar_caminho: flag indicando necessidade de criar um caminho
    :param reprocessar: flag indicando necessidade de reprocessamento dos dados
    """
    obj_etl = GetData(
        dataset=dataset,
        entrada=entrada,
        saida=saida,
        criar_caminho=criar_caminho,
        reprocessar=reprocessar,
        tabela='salary_data'
    )
    obj_etl.pipeline()
