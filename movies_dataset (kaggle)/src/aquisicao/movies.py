import abc
import typing
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm

from src.aquisicao._base import BaseMovieETL
from src.utils.info import carrega_yaml


class MoviesETL(BaseMovieETL, abc.ABC):
    """
    Classe responsável por realizar ETL dos dados do conjunto de dados dos filmes
    """
    _tabela: str
    _configs: typing.Dict[str, typing.Any]

    def __init__(self, entrada: Path, saida: Path, criar_caminho: bool = True, reprocessar: bool = False):
        super().__init__(entrada, saida, criar_caminho, reprocessar)

        self._tabela = 'movies'
        self._configs = carrega_yaml('aquisicao_movies.yml')

    def extract(self) -> None:
        """
        Extrai os dados
        """
        self._logger.info('CARREGANDO DADOS...')
        for tabela in tqdm([self._tabela]):
            self.dados_entrada[tabela] = pd.read_csv(self._cam_entrada / f'{tabela}.csv')

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
            self.trata_colunas(base, cols)
            self.dropa_colunas(base)
            self._logger.info('ELIMINANDO LINHAS NULAS')
            self.dropa_nulos(base)
            self._logger.info('SEPARANDO INFORMAÇÕES DA COLUNA "released"')
            self.trata_col_released(base)
            self._logger.info('PREENCHENDO VALORES NULOS')
            self.preenche_nulos(base)
            self._logger.info('SUBSTITUINDO VALORES DENTRO DAS COLUNAS')
            self.replace_values_in_cols(base)
            self._logger.info('REDUZINDO USO DE MEMÓRIA')
            self.converte_dtypes(base)
            self._logger.info('RENOMEANDO COLUNAS')
            self.renomeia_colunas(base)

            self.dados_saida[tabela] = base

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

    def trata_colunas(self, base: pd.DataFrame, cols: typing.List[str]) -> None:
        """
        Trata as colunas do conjunto de dados
        :param base: Dataframe a ser manipulado
        :param cols: Colunas a serem inseridas
        """
        base = base.assign(
            DadosJuntos=lambda f: f.iloc[:, 0] + f.iloc[:, 1]
        )
        base[cols] = base['DadosJuntos'].str.split(';', expand=True)

    def dropa_colunas(self, base: pd.DataFrame) -> None:
        """
        Dropa as colunas desnecessárias dentro do conjunto de dados
        :param base: Dataframe a ser manipulado
        """
        base.drop(columns=base.columns[:3], axis=1, inplace=True)

    def dropa_nulos(self, base: pd.DataFrame) -> None:
        """
        Dropa as linhas que são totalmente nulas
        :param base: Dataframe a ser manipulado
        """
        base.dropna(how='all', inplace=True)

    def preenche_nulos(self, base: pd.DataFrame) -> None:
        """
        Preenche nulos de uma coluna
        :param base: Dataframe a ser manipulado
        """
        base['runtime'].fillna(0, inplace=True)
        base['country'].fillna('País não informado', inplace=True)

    def trata_col_released(self, base: pd.DataFrame) -> None:
        """
        Trata a coluna "released"
        :param base: Dataframe a ser manipulado
        """
        base = base.assign(
            date=lambda f: pd.to_datetime((f['released'].str.split(' ', expand=True)[1] + '/' +
                                           f['released'].str.split(' ', expand=True)[0] + '/' + f['year']).str.replace(
                '"', ''), format='%d/%B/%Y'),
            country=lambda f: (
                        f['released'].str.split(' ', expand=True)[3] + ' ' + f['released'].str.split(' ', expand=True)[
                    4]).str.replace('["()]', '', regex=True)).drop(['released', 'year'], axis=1).replace('"', '',
                                                                                                         regex=True)

    def replace_values_in_cols(self, base: pd.DataFrame) -> None:
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

    def converte_dtypes(self, base: pd.DataFrame) -> None:
        """
        Converte os tipos de dados de cada coluna, afim de reduzir o uso de memória
        :param base: Dataframe a ser manipulado
        """
        base = base.assign(
            rating=lambda f: f['rating'].astype('category'),
            genre=lambda f: f['genre'].astype('category'),
            country=lambda f: f['country'].astype('category')
        )
        base = base.assign(
            score=lambda f: f['score'].astype(float),
            votes=lambda f: f['votes'].astype(float).astype(np.uint),
            budget=lambda f: f['budget'].astype(float),
            gross=lambda f: f['gross'].replace('', 0, regex=True).astype(float),
            runtime=lambda f: f['runtime'].astype(float).astype(np.uint8)
        )

    def renomeia_colunas(self, base: pd.DataFrame) -> None:
        """
        Renomeia as colunas do conjunto de dados
        :param base: Dataframe a ser manipulado
        """
        base.rename(
            columns=self._configs['RENOMEIA_COLUNAS'],
            inplace=True
        )
