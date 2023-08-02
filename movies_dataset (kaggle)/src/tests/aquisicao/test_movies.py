from pathlib import Path

import pandas as pd
import pytest

from src.aquisicao.movies import MoviesETL
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
def test_multiple_replaces(movies_etl: MoviesETL) -> None:
    pass
