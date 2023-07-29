import abc
import logging
import typing
from pathlib import Path

import pandas as pd
from tqdm import tqdm


class BaseMovieETL(abc.ABC):
    """
    Classe base para as demais classes responsáveis pelo ETL da base de dados dos filmes
    """
    _cam_entrada: Path
    _cam_saida: Path
    criar_caminho: bool
    _dados_entrada: typing.Dict[str, pd.DataFrame]
    _dados_saida: typing.Dict[str, pd.DataFrame]
    reprocessar: bool
    _logger: logging.Logger

    def __init__(self, caminho_entrada: Path, caminho_saida: Path, criar_caminho: bool = True,
                 reprocessar: bool = False) -> None:
        """
        :param caminho_entrada: Caminho de entrada dos dados
        :param caminho_saida: Caminho de saída dos dados
        :param criar_caminho: Flag indicando se necessidade de criar um caminho de entrada ou saida
        :param reprocessar: Flag indicando necessidade de reprocessar os dados
        """
        self._cam_entrada = caminho_entrada
        self.reprocessar = reprocessar
        self._cam_saida = caminho_saida
        self._dados_entrada = dict()
        self._dados_saida = dict()

        if criar_caminho:
            self._cam_entrada.mkdir(parents=True, exist_ok=True)
            self._cam_saida.mkdir(parents=True, exist_ok=True)

        self._logger = logging.getLogger(__name__)

    @property
    def dados_entrada(self) -> typing.Dict[str, pd.DataFrame]:
        """
        Permite o acesso o dicionário dos dados de entrada
        :return: Dicionário que contém os dados de entrada
        """
        if self._dados_entrada is not None:
            return self._dados_entrada

    @property
    def dados_saida(self) -> typing.Dict[str, pd.DataFrame]:
        """
        Permite o acesso o dicionário dos dados de saída
        :return: Dicionário que contém os dados de saída
        """
        if self._dados_saida is not None:
            return self._dados_saida

    @abc.abstractmethod
    def extract(self) -> None:
        """
        Extrai os dados
        """
        pass

    @abc.abstractmethod
    def transform(self) -> None:
        """
        Transforma os dados
        """
        pass

    def load(self) -> None:
        """
        Exporta os dados
        """
        self._logger.info('EXPORTANDO OS DADOS')
        for arq, df in tqdm([self._dados_saida.items()]):
            df.to_csv(self._cam_saida / arq, index=False)

    def pipeline(self) -> None:
        """
        Executa o pipeline dos dados
        """
        if self.reprocessar:
            self.extract()
            self.transform()
            self.load()
