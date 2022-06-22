# -*- coding: utf-8 -*-
import click
import logging
import requests
import pandas as pd
import geopandas as gpd
from pathlib import Path
from os.path import join
from datetime import date, timedelta
from dotenv import find_dotenv, load_dotenv

from src import root, project

def get_casos_confirmados(day=date.today(),tries=2, load=True):
    if not load:
        url = f"https://www.saude.pr.gov.br/sites/default/arquivos_restritos/files/documento/{day.strftime('%Y-%m')}/informe_epidemiologico_{day.strftime('%d_%m_%Y')}_geral.csv"
        try:
            print(f"GET {url}")
            cc = pd.read_csv(url, sep=';',low_memory=False)
            cc.to_pickle(join(project,'data','raw','casos_confirmados.pkl'),compression='bz2')
            return cc
        except:
            if tries > 0:
                return casos_confirmados(day-timedelta(1),tries - 1)
            else:
                raise
    else:
        try:
            return pd.read_pickle(join(project,'data','raw','casos_confirmados.pkl'),compression='bz2')
        except:
            return casos_confirmados(day,tries,False)

def get_geojson():
    url = 'https://servicodados.ibge.gov.br/api/v3/malhas/estados/41?formato=application/vnd.geo+json&qualidade=maxima&intrarregiao=municipio'
    with requests.get(url) as response:
        PR = response.json()

    gdf = gpd.GeoDataFrame.from_features(PR["features"])
    gdf = gdf.rename(columns={'codarea':'ibge7'})
    gdf['ibge7'] = gdf['ibge7'].astype(int)
    gdf.to_pickle(join(project,'data','external','geojson.pkl'))
    return gdf

def get_vacinacao():
    pass

def get_municipios():
    municipios = pd.read_csv('https://docs.google.com/spreadsheets/d/e/2PACX-1vSUBj51_ZxpyuDSo0jMqSwwwewNNu0G8nfv9seLFN4f_h5xKfNIRohfaXE2Yg2HAQ/pub?gid=1064518640&single=true&output=csv')
    municipios.to_pickle(join(project,'data','raw','municipios.pkl'))
    
    municipios = municipios.rename(columns={'UF': 'uf', 'COD. UF': 'cod_uf', 'COD. MUNIC': 'cod_mun', 'NOME DO MUNICÍPIO': 'municipio', ' POPULAÇÃO ESTIMADA': 'populacao'})
    municipios['ibge7'] = (municipios['cod_uf'].astype(str) + municipios['cod_mun'].astype(str).str.zfill(5)).astype(int)
    municipios['populacao'] = municipios['populacao'].str.replace(',','').str.replace('.','').str.split('(').apply(lambda x: x[0]).astype(int)
    municipios.set_index('ibge7',inplace=True)
    
    coordenadas = pd.read_csv('https://raw.githubusercontent.com/kelvins/Municipios-Brasileiros/main/csv/municipios.csv').set_index('codigo_ibge')
    coordenadas.to_pickle(join(project,'data','raw','coordenadas.pkl'))

    municipios = municipios.join(coordenadas[['latitude', 'longitude', 'capital', 'siafi_id', 'ddd', 'fuso_horario']])
    municipios.to_pickle(join(project,'data','external','municipios.pkl'))

@click.command()
@click.argument('force')
def main(force=False):
    """
        Get raw csv data from SESA and save compressed in (ch19dsh/data/raw)
    """
    logger = logging.getLogger(__name__)
    logger.info('making final data set from raw data')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    # project_dir = Path(__file__).resolve().parents[2]
    
    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
