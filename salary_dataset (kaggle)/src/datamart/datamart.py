import logging
from pathlib import Path
from ..aquisicao.funcoes import carrega_csv
import typing
import pandas as pd


class Datamart:
    """
    Objeto responsável pela criação de classes datamarts
    """
    _caminho_entrada_datamart: Path
    _dados_datamart: typing.Dict[str, pd.DataFrame]
    _tabela: str
    _logger: logging.Logger

    def __init__(self, caminho_entrada: Path, tabela: str) -> None:
        """
        :param caminho_entrada: Indica o caminho que leva aos dados do datamart, após o processamento.
        """
        self._caminho_entrada_datamart = caminho_entrada
        self._tabela = tabela
        self._logger = logging.getLogger(__name__)
        self._dados_datamart = dict()

    def __str__(self) -> str:
        """
        Representação de texto em classe
        """
        return self.__class__.__name__

    def _load_datamart(self) -> None:
        """
        Método protegido destinado ao carregamento do datamart
        """
        self._dados_datamart[self._tabela] = carrega_csv(self._caminho_entrada_datamart, self._tabela)

    def load_datamart(self) -> None:
        """
        Carrega o datamart
        """
        self._logger.info(f'CARREGANDO DADOS DO OBJETO {self}')
        self._load_datamart()

    def pipeline(self) -> None:
        """
        Executa o pipeline do datamart
        """
        self.load_datamart()
        self._logger.info(f'EXECTUANDO PIPELINE DO OBJETO {self}')

