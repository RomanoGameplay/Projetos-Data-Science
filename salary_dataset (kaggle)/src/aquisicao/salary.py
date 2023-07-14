import abc
import typing
import numpy as np
import pandas as pd
from base_etl import BaseETL
from info import carrega_yaml


class SALARYETL(BaseETL, abc.ABC):
    """
    Instancia objeto responsável por processamento da base de dados
    """
    _tabela: str
    _configs: typing.Dict[str, typing.Any]

    def __init__(self, entrada: str, saida: str, tabela: str, criar_caminho: bool = True) -> None:
        """
        Instancia o objeto de ETL Base
        :param entrada: String com o caminho para a pasta de entrada
        :param saida: String indicando o caminho para a pasta de saida
        :param tabela: Tabela de dados a ser processada
        :param criar_caminho: Flag indicando necessidade de criar caminho
        """
        super().__init__(entrada, saida, criar_caminho)
        self._tabela = tabela
        self._configs = carrega_yaml('aquisicao_salary.yml')

    def extract(self) -> None:
        """
        Extrai os dados do objeto
        """
        # carrega tabelas de interesse
        self._dados_entrada[self._tabela] = pd.read_csv(self._caminho_entrada / f'{self._tabela}.csv')

    def renomeia_colunas(self, base: pd.DataFrame) -> None:
        """
        Renomeia as colunas da base de entrada
        :param base: DataFrame a ser manipulado
        """
        base.rename(
            columns=self._configs['RENOMEIA_COLUNAS'],
            inplace=True
        )

    @classmethod
    def dropa_nulos(cls, base: pd.DataFrame, how=None) -> None:
        """
        Dropa valores nulos dentro do DataFrame
        :param base: DataFrame a ser manipulado
        :param how: define se removerá as linhas que contém apenas nulos, ou não
        """
        if how == 'all':
            base.dropna(how=how, inplace=True)
        else:
            base.dropna(inplace=True)

    @classmethod
    def preenche_nulos_em_salary(cls, base: pd.DataFrame) -> None:
        """
        Preenche dados nulos específicos na coluna "salary"
        :param base: DataFrame a ser manipulado
        """
        base.loc[lambda f: (f['Job Title'] == 'Sales Director') & (f['Salary'].isnull() == True) & (
                f['Years of Experience'] == 6.0), 'Salary'] = 75000.0
        base.loc[lambda f: (f['Job Title'] == 'Full Stack Engineer') & (f['Years of Experience'] == 8.0) & (
                f['Salary'].isnull() == True), 'Salary'] = 140000.0

    @classmethod
    def dropa_salarios(cls, base) -> None:
        """
        Dropa salários que estão abaixo do salário mínimo anual
        :param base: DataFrame a ser manipulado
        """
        ind = base[lambda f: f['Salary'] <= 16000].index
        base.drop(index=ind, inplace=True)

    @classmethod
    def substitue_valores(cls, base: pd.DataFrame, col: str, old_vals: typing.Tuple,
                          new_values: typing.Tuple) -> None:
        """
        Substitue os valores de determinada coluna
        :param base: DataFrame a ser manipulado
        :param col: Coluna alvo de substituição
        :param old_vals: Valores a serem substituidos
        :param new_values: Novos valores para substituir
        """
        vals = zip(old_vals, new_values)

        for v in vals:
            base[col].replace(v[0], v[1], inplace=True)

    @classmethod
    def converte_dtypes(cls, base) -> None:
        """
        Converte as colunas para reduzir o uso de memória
        :param base: DataFrame a ser manipulaado
        """
        base['Age'] = base['Age'].astype(np.uint)
        base['Years of Experience'] = base['Years of Experience'].astype(np.uint)
        base['Gender'] = base['Gender'].astype('category')
        base['Salary'] = base['Salary'].astype(np.uint)
        base['Education Level'] = base['Education Level'].astype('category')
        base['Job Title'] = base['Job Title'].astype('category')

    def transform(self) -> None:
        """
        Transforma os dados e os adequa para os formatos de saida de interesse
        """
        self.logger.info('Iniciando transformação do conjunto de dados')
        for tabela, base in self.dados_entrada.items():
            print(base)
            self.logger.info(f'Renomeando colunas de dados')
            self.renomeia_colunas(base)
            self.logger.info('Dropando dados nulos')
            self.dropa_nulos(base, how='all')
            self.logger.info('Preenchendo dados nulos')
            self.preenche_nulos_em_salary(base)
            self.logger.info('Dropando dados nulos restantes')
            self.dropa_nulos(base)
            self.logger.info('Dropando salários abaixo do valor mínimo anual')
            self.dropa_salarios(base)
            self.logger.info(f'Substituindo valores')
            self.substitue_valores(
                base,
                'Education Level',
                ("Master's", "Bachelor's", "phD"),
                ("Master's Degree", "bachelor's Degree", "PhD")
            )
