import abc
from src.aquisicao.base_etl import BaseBitocinUSDETL
from pathlib import Path
import pandas as pd


class BitcoinUSD(BaseBitocinUSDETL, abc.ABC):
    """
    Classe que executa o processamento da base de dados do preco do bitcoin
    """
    _tabela: str

    def __init__(self, cam_entrada: Path, cam_saida: Path, criar_caminho: bool = True) -> None:
        super().__init__(cam_entrada=cam_entrada, cam_saida=cam_saida, criar_caminho=criar_caminho)
        self._tabela = 'bitcoin_usd'

    def extract(self) -> None:
        """
        Extrai os dados
        """
        pass

    def transform(self) -> None:
        """
        Transforma os dados
        """
        pass

    def load(self) -> None:
        """
        Exporta os dados
        """
        pass
