import abc
from pathlib import Path
import typing
import pandas as pd
from .base_etl import BaseETL


class SALARYETL(BaseETL, abc.ABC):
    """
    Classe responsável por estruturar como qualquer objeto de ETL
    deve funcionar para armazenar dados do dataset de salários
    """

    _caminho_entrada: Path
    _caminho_saida: Path
    _dados_entrada: typing.Dict[str, pd.DataFrame]
    _dados_saida: typing.Dict[str, pd.DataFrame]

    def __init__(self, entrada: str, saida: str, criar_caminho: bool = True) -> None:
        """
        Instancia o objeto de ETL Base
        :param entrada: String com o caminho para a pasta de entrada
        :param saida: String indicando o caminho para a pasta de saida
        :param criar_caminho: Flag indicando necessidade de criar caminho
        """
        super().__init__(entrada, saida, criar_caminho)
        self._dados_entrada = None
        self._dados_saida = None
        self._caminho_entrada = Path(entrada)
        self._caminho_saida = Path(saida)
        if criar_caminho:
            self._caminho_entrada.mkdir(parents=True, exist_ok=True)
            self._caminho_saida.mkdir(parents=True, exist_ok=True)


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
