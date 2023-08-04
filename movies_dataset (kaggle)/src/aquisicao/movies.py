import abc
import typing
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from src.aquisicao._base import BaseMovieETL


class MoviesETL(BaseMovieETL, abc.ABC):
    """
    Classe responsável por realizar ETL dos dados do conjunto de dados dos filmes
    """
    _tabela: str
    _configs: typing.Dict[str, typing.Any]

    def __init__(self, entrada: Path, saida: Path, criar_caminho: bool = True, reprocessar: bool = False):
        super().__init__(entrada, saida, criar_caminho, reprocessar)
        self._tabela = 'movies'

    def extract(self) -> None:
        """
        Extrai os dados
        """
        self._logger.info('CARREGANDO DADOS...')
        for tabela in tqdm([self._tabela]):
            self.dados_entrada[tabela] = pd.read_csv(self._cam_entrada / f'{tabela}.csv')

    def multiple_replaces(self, string: str, replacements: typing.List[typing.Tuple[str, str]]) -> str:
        """
        Método que realiza múltiplas substituições numa string
        :param string: String a receber as múltiplas substituições
        :param replacements: lista de tuplas de dois valores, com a substring a ser substituida e a nova substring
        :return: Retorna uma string com os caracteres substituídos
        """
        for old, new in replacements:
            string = string.replace(old, new)
        return string

    @classmethod
    def adiciona_colunas(cls, base: pd.DataFrame, cols: list) -> pd.DataFrame:
        """
        Trata as colunas do conjunto de dados
        :param base: Dataframe a ser manipulado
        :param cols: Lista de colunas a serem inseridas
        :return: Retorna o dataframe com as colunas adicionadas
        """
        base = base.assign(
            DadosJuntos=lambda f: f.iloc[:, 0] + f.iloc[:, 1]
        )
        base[cols] = base['DadosJuntos'].str.split(';', expand=True)
        return base

    @classmethod
    def dropa_colunas(cls, base: pd.DataFrame) -> None:
        """
        Dropa as colunas desnecessárias dentro do conjunto de dados
        :param base: Dataframe a ser manipulado
        """
        base.drop(columns=base.columns[:4], axis=1, inplace=True)

    @classmethod
    def dropa_nulos(cls, base: pd.DataFrame) -> None:
        """
        Dropa as linhas que são totalmente nulas
        :param base: Dataframe a ser manipulado
        """
        base.dropna(how='all', inplace=True)

    @classmethod
    def preenche_nulos(cls, base: pd.DataFrame) -> None:
        """
        Preenche nulos de uma coluna
        :param base: Dataframe a ser manipulado
        """
        base['runtime'].fillna(0, inplace=True)
        base['country'].fillna('País não informado', inplace=True)

    @classmethod
    def trata_col_released(cls, base: pd.DataFrame) -> pd.DataFrame:
        """
        Trata a coluna "released"
        :param base: Dataframe a ser manipulado
        :return: Retorna um datraframe após os tratamentos
        """
        base = base.assign(
            date=lambda f: pd.to_datetime((f['released'].str.split(' ', expand=True)[1] + '/' +
                                           f['released'].str.split(' ', expand=True)[0] + '/' + f['year']).str.replace(
                '"', ''), format='%d/%B/%Y'),
            country=lambda f: (
                    f['released'].str.split(' ', expand=True)[3] + ' ' + f['released'].str.split(' ', expand=True)[
                4]).str.replace('["()]', '', regex=True)).drop(['released', 'year'], axis=1).replace('"', '',
                                                                                                     regex=True)
        return base

    @classmethod
    def replace_values_in_cols(cls, base: pd.DataFrame) -> None:
        """
        Substitui valores dentro das colunas
        :param base: Dataframe a ser manipulado
        """
        vals_to_replace = [('X', 'NC-17'), ('Not Rated', 'NR'), ('Unrated', 'UR'),
                           ('Approved', 'This Film Is Not Yet Rated'),
                           ('TV-PG', 'PG'), ('TV-MA', 'R'), ('', 'This Film Is Not Yet Rated')]
        for vals in vals_to_replace:
            base['rating'] = base['rating'].replace(vals[0], vals[1], regex=True)

        vals_to_replace = [('Federal Republic', 'País não informado'), ('Soviet Union', 'Sovietic Union (Russia)'),
                           ('West Germany', 'West Germany (Germany)')]
        for vals in vals_to_replace:
            base['country'] = base['country'].replace(vals[0], vals[1], regex=True)

    @classmethod
    def converte_dtypes(cls, base: pd.DataFrame) -> None:
        """
        Converte os tipos de dados de cada coluna, afim de reduzir o uso de memória
        :param base: Dataframe a ser manipulado
        :return: Retorna um dataframe com os dtypes convertidos
        """
        base['score'] = base['score'].astype(float)
        base['votes'] = base['votes'].astype(float)
        base['budget'] = base['budget'].astype(float)
        base['gross'] = base['gross'].replace('', 0, regex=True).astype(float)
        base['runtime'] = base['runtime'].astype(float).astype(int)

    @classmethod
    def dropa_string_vazia(cls, base: pd.DataFrame) -> None:
        """
        Dropa linhas que contém strings vazias
        :param base: Dataframe a ser manipulado
        """
        cols = base.columns.values
        for col in cols:
            op = base.loc[lambda f: f[col] == '']
            if len(op) != 0:
                base.drop(op.index, inplace=True, axis='index')
                op = base.loc[lambda f: f[col] == '']

    def transform(self) -> None:
        """
        Transforma os dados
        """
        self._logger.info('INICIANDO TRANSFORMAÇÃO DOS DADOS...')
        for tabela, base in self.dados_entrada.items():
            self._logger.info('TRATANDO COLUNAS')
            cols = self.multiple_replaces(str(base.columns.values),
                                          [('"', ''), ('[', ''), (']', ''), (',', ''), ("'", ''), ('\n', ''),
                                           ('Unnamed: 1', ''), ('Unnamed: 2', '')]).strip().split(';')
            base = self.adiciona_colunas(base, cols)
            self.dropa_colunas(base)
            self._logger.info('ELIMINANDO LINHAS NULAS')
            self.dropa_nulos(base)
            self._logger.info('SEPARANDO INFORMAÇÕES DA COLUNA "released"')
            base = self.trata_col_released(base)
            self._logger.info('PREENCHENDO VALORES NULOS')
            self.preenche_nulos(base)
            self._logger.info('SUBSTITUINDO VALORES DENTRO DAS COLUNAS')
            self.replace_values_in_cols(base)
            self._logger.info('CONVERTENDO OS TIPOS DE DADOS')
            self.converte_dtypes(base)
            self._logger.info('DROPANDO STRINGS VAZIAS')
            self.dropa_string_vazia(base)
            self._dados_saida[tabela] = base
