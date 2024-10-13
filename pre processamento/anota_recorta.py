# -*- coding: utf-8 -*-
# Objetivo deste código: Criar um dataset csv com base no dataset Hurdat2 contendo a 
#anotação do max_sus_wind_speed (variável a ser predita), categoria e o caminho de cada imagem do dataset Hursat

import os
import pandas as pd
import h5py
import netCDF4 as nc
from datetime import datetime
from tqdm import tqdm


def categorize_wind_speed(speed):

    categories = [
        (137, 'H5'), (113, 'H4'), (96, 'H3'),
        (83, 'H2'), (64, 'H1'), (34, 'TS')
    ]
    for limiar, categoria in categories:
        if speed >= limiar:
            return categoria
    return 'TD'  

def carregar_dados_hurdat(csv_path):
    data = pd.read_csv(csv_path)
    data['max_sus_wind_speed'] = pd.to_numeric(data['max_sus_wind_speed'], errors='coerce').fillna(0)
    data['name'] = data['name'].str.strip().str.upper()
    data['fulldate'] = data['fulldate'].astype(str).str.strip()
    data['time'] = data['time'].astype(str).str.strip()
    return data

def processar_nc_file(nc_file, data, nc_dir, h5_dir, dataset):
    parts = nc_file.split('.')
    name = parts[1].strip().upper()
    date_str = parts[2] + parts[3] + parts[4]
    time_str = parts[5][:4]

    matched_data = data[(data['name'] == name) &
                        (data['fulldate'] == date_str) &
                        (data['time'] == time_str)]
    
    if not matched_data.empty:
        print(f"Imagem com rótulo encontrado: {name} em {date_str} {time_str}")
        nc_path = os.path.join(nc_dir, nc_file)
        h5_path = os.path.join(h5_dir, nc_file.replace('.nc', '.h5')).replace('\\', '/')

        with nc.Dataset(nc_path, 'r') as nc_data:
            irwin_data = nc_data.variables['IRWIN'][0]
            cropped_image = crop_image(irwin_data, 100, 100)
            
            with h5py.File(h5_path, 'w') as h5_file:
                h5_file.create_dataset('IRWIN', data=cropped_image)

        wind_speed = matched_data['max_sus_wind_speed'].iloc[0]
        wind_category = categorize_wind_speed(wind_speed)

        dataset.append({
            'id':           matched_data['id'].iloc[0],
            'wind':          wind_speed,
            'wind_category': wind_category,
            'path':          h5_path
        })
    else:
        print(f"Rótulo não localizado para: {name} em {date_str} {time_str}")

def crop_image(image, height, width):
    starty = image.shape[0] // 2 - height // 2
    startx = image.shape[1] // 2 - width // 2
    return image[starty:starty + height, startx:startx + width]

def main():
    csv_file_path = r'G:\TCC\HURSAT\codigos_fontes\hurdat.csv'
    nc_directory = r'G:\TCC\HURSAT\codigos_fontes\hursat_download'
    output_csv_path = 'dataset-hursat-hurdat.csv'
    h5_directory = r'G:\TCC\HURSAT\codigos_fontes\hursat-hurdat'

    os.makedirs(h5_directory, exist_ok=True)

    data = carregar_dados_hurdat(csv_file_path)
    print(data.head())

    dataset = []
    nc_files = [f for f in os.listdir(nc_directory) if f.endswith('.nc')]

    for nc_file in tqdm(nc_files, desc="Processando arquivos"):
        processar_nc_file(nc_file, data, nc_directory, h5_directory, dataset)

    df = pd.DataFrame(dataset)
    df.to_csv(output_csv_path, index=False)

    print(f"Dataset criado com sucesso e salvo em {output_csv_path}")

if __name__ == "__main__":
    main()