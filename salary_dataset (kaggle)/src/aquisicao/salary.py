import abc
from pathlib import Path
import typing
import pandas as pd
from base_etl import BaseETL


class SALARYETL(BaseETL, abc.ABC):
    """
    Instancia objeto responsável por processamento da base de dados
    """
    _tabela: str

    def __init__(self, entrada: str, saida: str, tabela: str, criar_caminho: bool = True) -> None:
        """
        Instancia o objeto de ETL Base
        :param entrada: String com o caminho para a pasta de entrada
        :param saida: String indicando o caminho para a pasta de saida
        :param tabela: Tabela de dados a ser processada
        :param criar_caminho: Flag indicando necessidade de criar caminho
        """
        super().__init__(entrada, saida, criar_caminho)
        self._tabela = tabela

    def extract(self) -> None:
        """
        Extrai os dados do objeto
        """
        # carrega tabelas de interesse
        self._dados_entrada[self._tabela] = pd.read_csv(self._caminho_entrada / f'{self._tabela}.csv')

    def transform(self) -> None:
        """
        Transforma os dados e os adequa para os formatos de saida de interesse
        """
        pass
