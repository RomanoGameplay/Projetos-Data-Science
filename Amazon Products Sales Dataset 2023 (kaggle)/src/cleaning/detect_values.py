import pandas as pd


def search_anomalies_by_standard_deviation(df: pd.DataFrame, column: str, show_df_values: bool=True) -> list:
    '''
    :param df: DataFrame a ser manipulado.
    :param column: Coluna específica onde serão encontrados os outliers.
    :param show_df_values: Permite mostrar os valores dentro do DataFrame considerados outliers.
    :return: lista de valores considerados como outliers dentro do DataFrame.
    '''
    # define uma lista para armazenar valores considerados anomalias.
    anomalies = []

    # Definindo limites superior e inferior para desvio padrão 3.
    random_data_std = df[column].std()
    random_data_mean = df[column].mean()
    anomaly_cut_off = random_data_std * 3
    lower_limit = random_data_mean - anomaly_cut_off
    upper_limit = random_data_mean + anomaly_cut_off

    # Gera outliers
    for outlier in df[column]:
        if outlier > upper_limit or outlier < lower_limit:
            anomalies.append(outlier)

    if show_df_values:
        print(df.loc[df[column].isin(anomalies)])

    return anomalies


def by_boxplot(df: pd.DataFrame, column: str) -> None:
    import seaborn as sns

    sns.boxplot(data=df[column])


def detect_special_characters(df: pd.DataFrame, column: str) -> list:
    '''
    Detecta dinamicamente caracteres especiais em uma coluna do dataframe.
    :param df: O dataframe a ser verificado.
    :param column: O nome da coluna a ser analisada.
    :return: set
    '''
    import re

    caracteres_especiais = set()

    for valor in df[column]:
        if isinstance(valor, str):
            matches = re.findall(r'[^\w\s.]', valor)
            caracteres_especiais.update(matches)
    return list(caracteres_especiais)
