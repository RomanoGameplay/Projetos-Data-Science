from src.utils.info import carrega_yaml


def test_carrega_yaml() -> None:
    info = carrega_yaml('aquisicao_movies.yml')
    assert isinstance(info, dict)
    assert 'RENOMEIA_COLUNAS' in info
    assert 'DADOS_SCHEMA' in info
