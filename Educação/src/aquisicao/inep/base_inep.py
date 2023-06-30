import abc
import os
import typing
import urllib
from pathlib import Path

from bs4 import BeautifulSoup
from pandas import DataFrame

from ....src.aquisicao.base_etl import BaseETL
from ....src.utils.web import download_dados_web


class BaseINEPETL(abc.ABC, BaseETL):
    """
    Classe que estrutura como qualuqer objeto ETL deve funcionar
    para baixar dados do INEP
    """
    caminho_entrada: Path
    caminho_saida: Path
    _dados_entrada: typing.Dict[str, DataFrame]
    _dados_saida: typing.Dict[str, DataFrame]
    URL: str = 'https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/'

    def __init__(self, entrada: str, saida: str, base: str, criar_caminho: bool = True) -> None:
        """
        Instancia o objeto de ETL Base
        :param entrada: string com o caminho para a pasta de entrada
        :param saida: string com o caminho para a pasta de saida
        :param base: nome da base que vai a base do INEP
        :param criar_caminho: flag indicando se devemos criar os caminhos
        """
        super().__init__(entrada, saida, criar_caminho)
        self._url = f'{self.URL}/{base}'

    def le_pagina_inep(self) -> typing.Dict[str, str]:
        """
        Realiza o web scrapping da página de dados do INEP

        :return: dicionário com o nome do arquivo e link para página
        """

        html = urllib.request.urlopen(self._url).read()
        soup = BeautifulSoup(html, features='html.parser')
        return {tag['href'].split('_')[-1]: tag['href'] for tag in soup.find_all('a', {'class': 'external-link'})}

    def dicionario_para_baixar(self) -> typing.Dict[str, str]:
        """
        Le os conteúdos da pasta de dados e seleciona apenas os arquivos a serem baixados como complementares
        :return: dicionário com o nome do arquivo e link para página
        """
        para_baixar = self.le_pagina_inep()
        baixados = os.listdir(str(self.caminho_entrada))
        return {arq: link for arq, link in para_baixar.items() if arq not in baixados}

    def donwload_conteudo(self) -> None:
        """
        Realiza o donwload dos dados INEP para uma pasta local
        """
        for arq, link in self.dicionario_para_baixar():
            caminho_arq = self.caminho_saida / arq
            download_dados_web(caminho_arq, link)

    @abc.abstractmethod
    def extract(self) -> None:
        """
        Extrai os dados do objeto
        :return:
        """
        pass

    @abc.abstractmethod
    def transform(self) -> None:
        """
        Transforma os dados e os adequa para os formatos de saída de interesse.
        """
        pass

    def load(self) -> None:
        """
        Exporta os dados transformados
        """
        for arq, df in self._dados_saida.items():
            df.to_parquet(self.caminho_saida / arq, index=False)

    def pipeline(self) -> None:
        """
        Executa o pipeline completo de tratamento de dados
        """
        self.extract()
        self.transform()
        self.load()
