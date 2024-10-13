# -*- coding: utf-8 -*-
# Objetivo deste código: Remover as imagens do dataset Hursab que estiverem danificadas

import os
import netCDF4
from tqdm import tqdm
import numpy as np
import time
import shutil

# Configurações
NC_DIRECTORY = r'G:\TCC\HURSAT\codigos_fontes\hursat_download'
DANIFICADOS_DIRECTORY = 'danificadas'  # Pasta para onde os arquivos danificados serão movidos
LIMIAR_DANIFICACAO = 1  # Número mínimo de pixels vazios consecutivos para considerar uma falha

os.makedirs(DANIFICADOS_DIRECTORY, exist_ok=True)

def analisar_imagem(imagem, limiar):
    for i in range(imagem.shape[0]):
        linha = imagem[i, :]
        vazios = np.ma.getmask(linha) if np.ma.is_masked(linha) else np.isnan(linha)
        grupos = np.split(vazios, np.where(np.diff(vazios) != 0)[0] + 1)
        if any(grupo.all() and len(grupo) >= limiar for grupo in grupos):
            return True
    return False

def processar_arquivo(arquivo):
    caminho_arquivo = os.path.join(NC_DIRECTORY, arquivo)
    try:
        with netCDF4.Dataset(caminho_arquivo, 'r') as nc_data:
            imagem = nc_data.variables['IRWIN'][0]
            starty, startx = imagem.shape[0]//2 - 112, imagem.shape[1]//2 - 112
            imagem_cortada = imagem[starty:starty+224, startx:startx+224]

            if analisar_imagem(imagem_cortada, LIMIAR_DANIFICACAO):
                shutil.move(caminho_arquivo, os.path.join(DANIFICADOS_DIRECTORY, arquivo))
                return True
    except Exception as e:
        print(f"Erro ao processar o arquivo {arquivo}: {e}")
    return False

def main():
    arquivos_nc = [f for f in os.listdir(NC_DIRECTORY) if f.endswith('.nc')]
    arquivos_danificados = []

    for arquivo in tqdm(arquivos_nc, desc="Verificando imagens"):
        if processar_arquivo(arquivo):
            arquivos_danificados.append(arquivo)
            time.sleep(1)

    print(f"Número de imagens com falhas encontradas: {len(arquivos_danificados)}")
    if arquivos_danificados:
        print("Imagens com falhas movidas para a pasta 'danificadas':")
        for arquivo in arquivos_danificados:
            print(arquivo)

if __name__ == "__main__":
    main()
