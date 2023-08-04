from pathlib import Path

import pandas as pd
import pytest

from src.aquisicao.movies import MoviesETL
from src.configs import PROJECT_PATH
from src.utils.info import carrega_yaml


INFO = carrega_yaml('aquisicao_movies.yml')


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
def movies_etl(entrada: Path, saida: Path) -> MoviesETL:
    etl = MoviesETL(
        entrada=entrada,
        saida=saida,
        criar_caminho=False
    )
    etl.dados_entrada['movies'] = pd.read_csv(entrada / 'movies.csv')
    return etl


@pytest.mark.order1
def test_extract(movies_etl: MoviesETL) -> None:
    movies_etl.extract()
    assert movies_etl.dados_entrada is not None
    assert 'movies' in movies_etl.dados_entrada.keys()
    assert isinstance(movies_etl.dados_entrada['movies'], pd.DataFrame)


@pytest.mark.order2
def test_adiciona_colunas(movies_etl: MoviesETL) -> None:
    base = movies_etl.dados_entrada['movies']
    cols = str(base.columns.values)
    replacements = [('"', ''), ('[', ''), (']', ''), (',', ''), ("'", ''), ('\n', ''), ('Unnamed: 1', ''),
                    ('Unnamed: 2', '')]
    new_cols = movies_etl.multiple_replaces(
        string=cols,
        replacements=replacements
    )
    assert isinstance(new_cols, str)
    new_cols = new_cols.strip().split(';')
    assert isinstance(new_cols, list)

    base = movies_etl.adiciona_colunas(
        base=base,
        cols=new_cols
    )

    for col in new_cols:
        assert col in base.columns.values

    movies_etl.dados_entrada['movies'] = base


@pytest.mark.order3
def test_dropa_colunas(movies_etl: MoviesETL) -> None:
    name_col = 'name;"rating";"genre";"year";"released";"score";"votes";"director";"writer";"star";"country";' \
               '"budget";"gross";"company";"runtime"'
    base = movies_etl.dados_entrada['movies']
    cols = ['Unnamed: 0', 'Unnamed: 1', 'DadosJuntos',
            name_col]
    movies_etl.dropa_colunas(base)
    assert cols not in base.columns.values


@pytest.mark.order4
def test_dropa_nulos(movies_etl: MoviesETL) -> None:
    base = movies_etl.dados_entrada['movies']
    num_nulos_antes = base.isnull().sum().sum()
    movies_etl.dropa_nulos(
        base=base
    )
    num_nulos_depois = base.isnull().sum().sum()
    assert num_nulos_antes != num_nulos_depois


@pytest.mark.order5
def test_trata_col_released(movies_etl: MoviesETL) -> None:
    new_cols = ['date', 'country']
    base = movies_etl.dados_entrada['movies']
    base = movies_etl.trata_col_released(base)
    for col in new_cols:
        assert col in base.columns
    movies_etl.dados_entrada['movies'] = base


@pytest.mark.order6
def test_preenche_nulos(movies_etl: MoviesETL) -> None:
    base = movies_etl.dados_entrada['movies']
    movies_etl.preenche_nulos(base)
    assert base['runtime'].isnull().sum().sum() == 0
    assert base['country'].isnull().sum().sum() == 0
