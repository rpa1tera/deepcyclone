# -*- coding: utf-8 -*-
# Objetivo deste código: baixar imagens de satélites do site
# ncei.noaa.gov e salvar em um diretório para processamento posterior.

import os
import requests
import tarfile
from bs4 import BeautifulSoup

def criar_diretorio_se_necessario(diretorio):
    if not os.path.exists(diretorio):
        os.makedirs(diretorio)

def obter_links_arquivos(ano):
    url_base = f'https://www.ncei.noaa.gov/data/hurricane-satellite-hursat-b1/archive/v06/{ano}'
    pagina = requests.get(url_base).text
    soup = BeautifulSoup(pagina, 'html.parser')
    return [url_base + '/' + node.get('href') for node in soup.find_all('a') if node.get('href').endswith('tar.gz')]

def baixar_arquivo(url, caminho_destino):
    resposta = requests.get(url, allow_redirects=True)
    with open(caminho_destino, 'wb') as arquivo:
        arquivo.write(resposta.content)

def extrair_arquivos_tar(caminho_tar, diretorio_destino):
    arquivos_extraidos = set()
    with tarfile.open(caminho_tar) as tar:
        for membro in tar.getmembers():
            nome_basico = '.'.join(membro.name.split('.')[:6])
            if nome_basico not in arquivos_extraidos:
                arquivos_extraidos.add(nome_basico)
                tar.extract(membro, path=diretorio_destino)

def processar_ano(ano, diretorio_base):
    diretorio_ano = os.path.join(diretorio_base, ano)
    criar_diretorio_se_necessario(diretorio_ano)

    links_arquivos = obter_links_arquivos(ano)
    total_arquivos = len(links_arquivos)
    imagens_extraidas = 0

    for i, url_arquivo in enumerate(links_arquivos, start=1):
        nome_arquivo = os.path.join(diretorio_ano, os.path.basename(url_arquivo))
        baixar_arquivo(url_arquivo, nome_arquivo)
        extrair_arquivos_tar(nome_arquivo, diretorio_ano)
        os.remove(nome_arquivo)
        imagens_extraidas += 1
        imprime_progresso(f'Processando arquivos de {ano}', i, total_arquivos)

    print(f'\nAno: {ano} - Total de furacões: {total_arquivos}, Total de imagens: {imagens_extraidas}')

def imprime_progresso(acao, progresso, total):
    percentual = round((progresso / total) * 100, 1)
    print(f'\r{acao}... {percentual}% ({progresso} de {total})', end='')

if __name__ == "__main__":
    ANOS_BAIXAR = [str(ano) for ano in range(2006, 2017)]
    DIRETORIO_BASE = 'hursat_download'

    for ano in ANOS_BAIXAR:
        processar_ano(ano, DIRETORIO_BASE)
