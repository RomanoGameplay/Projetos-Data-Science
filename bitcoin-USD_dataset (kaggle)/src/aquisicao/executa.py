from src.aquisicao.bitcoin import BitcoinUSD
from pathlib import Path


def executa_etl(entrada: Path, saida: Path, criar_caminho: bool = True) -> None:
    """
    Função que executa ETL
    :param entrada: Caminho de entrada dos dados
    :param saida: Caminho de saida dos dados
    :param criar_caminho: Flag que indica necessidade de criar caminho dos dados
    """
    obj = BitcoinUSD(
        cam_entrada=entrada,
        cam_saida=saida,
        criar_caminho=criar_caminho
    )
    obj.pipeline()
