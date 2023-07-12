import logging
import typing
from pathlib import Path
import abc
import pandas as pd


class BaseETL(abc.ABC):
    """
    Classe responsável por instanciar o objeto que recuperará os dados
    """
    _logger: logging.Logger
    reprocessar: bool
    _caminho_entrada: Path
    _caminho_saida: Path
    _dados_entrada: typing.Dict[str, pd.DataFrame]
    _dados_saida: typing.Dict[str, pd.DataFrame]

    def __init__(self, entrada: Path, saida: Path, reprocessar: bool = True, criar_caminho: bool = True) -> None:
        """
        Método construtor da classe BaseETL

        :param entrada: Indica o caminho de entrada dos dados
        :param saida: Indica o caminho de saída dos dados
        :param reprocessar: Flag indicando se deve reprocessar os dados
        :param criar_caminho: Flag indicando se deve criar os caminhos
        """
        self._logger = logging.getLogger(__name__)
        self.reprocessar = reprocessar
        self._dados_entrada = dict()
        self._dados_saida = dict()
        self._caminho_entrada = Path(entrada)
        self._caminho_saida = Path(saida)

        if criar_caminho:
            self._caminho_entrada.mkdir(parents=True, exist_ok=True)
            self._caminho_saida.mkdir(parents=True, exist_ok=True)

    def __str__(self) -> str:
        """
        Representação de texto da classe
        """
        return self.__class__.__name__

    @abc.abstractmethod
    def _extract(self) -> None:
        """
        Método protegido destinado a extração dos dados
        """
        raise NotImplementedError

    @abc.abstractmethod
    def _transform(self) -> None:
        """
        Método protegido destinado a transformação dos dados
        """
        raise NotImplementedError

    @abc.abstractmethod
    def _export(self) -> None:
        """
        Método protegido destinado a exportação dos dados
        """

    def extract(self) -> None:
        """
        Carrega os dados
        """
        self._logger.info(f'EXTRAINDO DADOS DO OBJETO {self}')
        self._extract()

    def transform(self) -> None:
        """
        Transforma os dados carregados
        """
        self._logger.info(f'TRANSFORMANDO DADOS DO OBJETO {self}')
        self._transform()

    def export(self) -> None:
        """
        Exporta os dados transformados
        """
        self._logger.info(f'EXPORTANDO DADOS DO OBJETO {self}')
        if self.reprocessar:
            self._export()

    def pipeline(self) -> None:
        """
        Executa as tarefas de extração, transformação e exportação dos dados
        """
        self.extract()
        self.transform()
        self.export()
