import typing
import numpy as np
from .funcoes import *
from .base_etl import BaseETL


class GetData(BaseETL):
    """
    Classe responsável por carregar e manipular os dados
    """
    _tabela: str
    _configs = typing.Union[str, typing.Any]
    _dataset: str

    def __init__(self, dataset: str, entrada: Path, saida: Path, tabela: str, criar_caminho: bool = True,
                 reprocessar: bool = False) -> None:
        """
        Método construtor da classe GetData

        :param entrada: Caminho de entrada dos dados
        :param saida: Caminho de saída dos dados
        :param tabela: Tabela a ser processada
        :param criar_caminho: Flag que indicando se é necessário criar caminho
        :param reprocessar: Flag indicando se deve reprocessar a base de dados
        """
        super().__init__(criar_caminho=criar_caminho, reprocessar=reprocessar, entrada=entrada, saida=saida)
        self._tabela = tabela
        self._dataset = dataset

    def __str__(self) -> str:
        """
        Representação de texto da classe
        """
        return self.__class__.__name__

    def renomeia_colunas(self, base: pd.DataFrame) -> None:
        base.rename(columns={base.columns.values[0]: 'Age'}, inplace=True)

    def dropa_linhas_nulas(self, base: pd.DataFrame, how=None) -> None:
        if how is not None:
            base.dropna(how=how, inplace=True)
        else:
            base.dropna(inplace=True)

    def preenche_nulos(self, base: pd.DataFrame) -> None:
        base.loc[lambda f: (f['Job Title'] == 'Sales Director') & (f['Salary'].isnull() == True) & (
                    f['Years of Experience'] == 6.0), 'Salary'] = 75000.0
        base.loc[lambda f: (f['Job Title'] == 'Full Stack Engineer') & (f['Years of Experience'] == 8.0) & (
                    f['Salary'].isnull() == True), 'Salary'] = 140000.0

    def remove_salarios_abaixo_do_minimo(self, base: pd.DataFrame) -> None:
        ind = base[lambda f: f['Salary'] <= 16000].index
        base = base.drop(index=ind)

    def altera_valores(self, base: pd.DataFrame) -> None:
        base['Education Level'].replace("Master's", "Master's Degree", inplace=True)
        base['Education Level'].replace("Bachelor's", "bachelor's Degree", inplace=True)
        base['Education Level'].replace("phD", "PhD", inplace=True)

    def altera_dtypes(self, base: pd.DataFrame) -> None:
        base['Age'] = base['Age'].astype(np.uint)
        base['Years of Experience'] = base['Years of Experience'].astype(np.uint)
        base['Gender'] = base['Gender'].astype('category')
        base['Salary'] = base['Salary'].astype(np.uint)
        base['Education Level'] = base['Education Level'].astype('category')
        base['Job Title'] = base['Job Title'].astype('category')

    def _extract(self) -> None:
        """
        Extrai os dados
        """
        self._dados_entrada[self._tabela] = carrega_csv(self._caminho_entrada, self._dataset, encoding='latin-1')

    def _transform(self) -> None:
        """
        Transforma os dados
        """
        base = self._dados_entrada[self._tabela]

        self._logger.info('Renomeando colunas de interesse')
        self.renomeia_colunas(base)

        self._logger.info('Removendo linhas completamente nulas')
        self.dropa_linhas_nulas(base, how='all')

        self._logger.info('Preenchendo nulos')
        self.preenche_nulos(base)

        self._logger.info('Removendo linhas nulas restantes')
        self.dropa_linhas_nulas(base)

        self._logger.info('Removendo dados discrepantes')
        self.remove_salarios_abaixo_do_minimo(base)

        self._logger.info('Alterando valores')
        self.altera_valores(base)

        self._logger.info('Otimizando uso de memória alterando os tipos de dados')
        self.altera_dtypes(base)

        self._dados_saida[self._tabela] = base

    def _export(self) -> None:
        """
        Exporta os dados
        """
        for arq, df in self._dados_saida.items():
            df.to_csv(self._caminho_saida / f'{self._tabela}.csv', index=False)
