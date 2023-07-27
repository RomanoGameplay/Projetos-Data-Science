from src.aquisicao.movies import MoviesETL
from enum import Enum


class EnumMovies(Enum):
    movies = 'MOVIES'


ETL_DICT = {
    EnumMovies.movies: MoviesETL
}
