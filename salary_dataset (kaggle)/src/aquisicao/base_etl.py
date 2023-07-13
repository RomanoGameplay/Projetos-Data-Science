import abc
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

    def __init__(self, entrada: str, saida: str) -> None:
        self._caminho_entrada = Path(entrada)
        self._caminho_saida = Path(saida)
        self._dados_entrada = dict()
        self._dados_saida = dict()

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
