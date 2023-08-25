import abc
from src.aquisicao.base_etl import BaseBitocinUSDETL
from pathlib import Path
import pandas as pd
from tqdm import tqdm


class BitcoinUSD(BaseBitocinUSDETL, abc.ABC):
    """
    Classe que executa o processamento da base de dados do preco do bitcoin
    """
    _tabela: str

    def __init__(self, cam_entrada: Path, cam_saida: Path, criar_caminho: bool = True) -> None:
        super().__init__(cam_entrada=cam_entrada, cam_saida=cam_saida, criar_caminho=criar_caminho)
        self._tabela = 'BTC-USD'

    def extract(self) -> None:
        """
        Extrai os dados
        """
        for tabela in tqdm([self._tabela]):
            self.dados_entrada[tabela] = pd.read_csv(self._cam_entrada / f'{tabela}.csv')

    @classmethod
    def converte_col_date(cls, base) -> None:
        """
        Converte a coluna "Date" para o tipo datetime
        :param base: DataFrame a ser manipulado
        """
        base['Date'] = pd.to_datetime(base['Date'])

    def transform(self) -> None:
        """
        Transforma os dados
        """
        for tabela, base in self.dados_entrada.items():
            self._logger.info('\t-> Convertendo coluna "Date" para o tipo Datetime')
            self.converte_col_date(base)
            self.dados_saida[tabela] = base

    def load(self) -> None:
        """
        Exporta os dados
        """
        for tabela in tqdm([self._tabela]):
            self.dados_saida[tabela].to_csv(self._cam_saida / f'{tabela}.csv', index=False)
