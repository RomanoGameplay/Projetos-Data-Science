from pathlib import Path

import pandas as pd
import pytest

from src.aquisicao.salary import SalaryETL
from src.configs import PROJECT_PATH


@pytest.fixture(scope='module')
def base_path() -> Path:
    base_path = Path(PROJECT_PATH)
    return base_path


@pytest.fixture(scope='module')
def entrada(base_path: Path) -> Path:
    entrada = base_path / 'dados' / 'entrada'
    return entrada


@pytest.fixture(scope='module')
def saida(base_path: Path) -> Path:
    saida = base_path / 'dados' / 'saida'
    return saida


@pytest.fixture(scope='module')
def salary_etl(entrada: Path, saida: Path) -> SalaryETL:
    etl = SalaryETL(
        entrada=entrada,
        saida=saida,
        criar_caminho=False
    )
    etl.dados_entrada['Salary_Data'] = pd.read_csv(entrada / 'Salary_Data.csv')
    return etl


@pytest.mark.order1
def test_extract(salary_etl: SalaryETL) -> None:
    salary_etl.extract()
    assert salary_etl.dados_entrada is not None
    assert 'Salary_Data' in salary_etl.dados_entrada.keys()
    assert isinstance(salary_etl.dados_entrada['Salary_Data'], pd.DataFrame)


@pytest.mark.order2
def test_renomeia_colunas(salary_etl: SalaryETL) -> None:
    columns = ['Age', 'Salary', 'Years of Experience', 'Gender', 'Education Level', 'Job Title']
    salary_etl.renomeia_colunas(salary_etl.dados_entrada['Salary_Data'])
    for col in columns:
        assert col in salary_etl.dados_entrada['Salary_Data'].columns


@pytest.mark.order3
def test_dropa_nulos(salary_etl: SalaryETL) -> None:
    # Dropa linhas completamente nulas
    base = salary_etl.dados_entrada['Salary_Data']
    num_nulos_antes = base.isnull().sum().sum()
    salary_etl.dropa_nulos(how='all', base=base)
    num_nulos_depois = base.isnull().sum().sum()
    # Verifica se houve remoção dos dados nulos
    assert num_nulos_antes != num_nulos_depois

    # Dropa linhas com algum dado nulo
    salary_etl.dropa_nulos(base=base)
    assert 0 == base.isnull().sum().sum()


@pytest.mark.order4
def test_converte_dtypes(salary_etl: SalaryETL) -> None:

    base = salary_etl.dados_entrada['Salary_Data']
    salary_etl.dropa_nulos(how='all', base=base)
    salary_etl.dropa_nulos(base=base)

    salary_etl.converte_dtypes(base)
    col_dtypes = salary_etl.dados_entrada['Salary_Data'].dtypes
    new_col_dtypes = ('uint64', 'category')

    assert col_dtypes[0] == new_col_dtypes[0]
    assert col_dtypes[1] == new_col_dtypes[1]
    assert col_dtypes[2] == new_col_dtypes[1]
    assert col_dtypes[3] == new_col_dtypes[1]
    assert col_dtypes[4] == new_col_dtypes[0]
    assert col_dtypes[5] == new_col_dtypes[0]
