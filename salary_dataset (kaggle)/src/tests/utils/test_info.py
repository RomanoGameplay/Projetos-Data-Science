from src.utils.info import carrega_yaml


def test_carrega_yaml():
    info = carrega_yaml('aquisicao_salary.yml')
    assert isinstance(info, dict)
    assert 'RENOMEIA_COLUNAS' in info