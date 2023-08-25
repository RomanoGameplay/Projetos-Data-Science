import abc
import logging
import os
from pathlib import Path
import typing

import pandas as pd


class BaseBitocinUSDETL(abc.ABC):
    """
    Classe esqueleto responsável por estruturar o ETL da base de dados do preço do bitcoin
    """
    _dados_entrada: typing.Dict[str, pd.DataFrame]
    _dados_saida: typing.Dict[str, pd.DataFrame]
    _cam_entrada: Path
    _cam_saida: Path
    criar_caminho: bool
    _logger: logging.Logger

    def __init__(self, cam_entrada: Path, cam_saida: Path, criar_caminho: bool = True) -> None:
        """
        Método construtor da classe
        :param cam_entrada: Caminho de entrada dos dados
        :param cam_saida: Caminho de saida dos dados
        :param criar_caminho: Flag que indica necessidade de criar caminho dos dados
        """
        self._cam_entrada = cam_entrada
        self._cam_saida = cam_saida
        self._dados_entrada = dict()
        self._dados_saida = dict()
        self._logger = logging.getLogger(__name__)

        if criar_caminho:
            self._cam_entrada.mkdir(parents=True, exist_ok=True)
            self._cam_saida.mkdir(parents=True, exist_ok=True)

    @property
    def dados_entrada(self) -> typing.Dict[str, pd.DataFrame]:
        if self._dados_entrada is not None:
            return self._dados_entrada

    @property
    def dados_saida(self) -> typing.Dict[str, pd.DataFrame]:
        if self._dados_saida is not None:
            return self._dados_saida

    @abc.abstractmethod
    def extract(self) -> None:
        """
        Extrai os dados de entrada
        """
        pass

    @abc.abstractmethod
    def transform(self) -> None:
        """
        Transforma os dados
        """
        pass

    @abc.abstractmethod
    def load(self) -> None:
        """
        Armazena os dados transformados
        """
        pass

    def pipeline(self) -> None:
        """
        Executa o pipeline dos dados
        """
        self.extract()
        self.transform()
        self.load()
