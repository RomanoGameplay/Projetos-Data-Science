import abc
import logging
from pathlib import Path
import typing
import pandas as pd


class BaseETL(abc.ABC):
    """
    Classe responsável por estruturar como qualquer objeto de ETL
    deve funcionar
    """

    _caminho_entrada: Path
    _caminho_saida: Path
    _dados_entrada: typing.Dict[str, pd.DataFrame]
    _dados_saida: typing.Dict[str, pd.DataFrame]
    logger: logging.Logger

    def __init__(self, entrada: Path, saida: Path, criar_caminho: bool = True) -> None:
        """
        Instancia o objeto de ETL Base
        :param entrada: String com o caminho para a pasta de entrada
        :param saida: String indicando o caminho para a pasta de saida
        :param criar_caminho: Flag indicando necessidade de criar caminho
        """
        self._dados_entrada = dict()
        self._dados_saida = dict()
        self._caminho_entrada = entrada
        self._caminho_saida = saida
        if criar_caminho:
            self._caminho_entrada.mkdir(parents=True, exist_ok=True)
            self._caminho_saida.mkdir(parents=True, exist_ok=True)

        self.logger = logging.getLogger(__name__)

    @property
    def dados_entrada(self) -> typing.Dict[str, pd.DataFrame]:
        """
        Acessa o dicionário com os dados de entrada
        :return: Dicionário com o nome do arquivo de um dataframe com os dados de entrada
        """
        if self._dados_entrada is None:
            self.extract()
        return self._dados_entrada

    @property
    def dados_saida(self) -> typing.Dict[str, pd.DataFrame]:
        """
        Acessa o dicionário com os dados de saida
        :return: Dicionário com o nome do arquivo de um dataframe com os dados de saida
        """
        if self._dados_saida is None:
            self.extract()
        return self._dados_saida

    @abc.abstractmethod
    def extract(self) -> None:
        """
        Extrai os dados do objeto
        """
        pass

    @abc.abstractmethod
    def transform(self) -> None:
        """
        Transforma os dados e os adequa para os formatos de saida de interesse
        """
        pass

    def load(self) -> None:
        """
        Exporta os dados transformados
        """
        for arq, df in self._dados_saida.items():
            df.to_csv(self._caminho_saida / arq, index=False)

    def pipeline(self) -> None:
        """
        Executa o pipeline completo de tratamento dos dados
        """
        self.extract()
        self.transform()
        self.load()
