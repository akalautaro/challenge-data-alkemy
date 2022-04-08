import os
import requests
import csv
import pandas as pd
import datetime
import locale
from logger_base import log

# Configuraciones iniciales y algunas constantes
locale.setlocale(locale.LC_TIME, '')  # Esto para que el nombre de los meses sea en español
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
FECHA_ACTUAL = datetime.date.today().strftime('%d-%m-%Y')
DATE_DIRNAME = datetime.date.today().strftime('%Y-%B')

DATA_DICT = [{'categoria': 'museos',
              'url': 'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/4207def0-2ff7-41d5-9095-d42ae8207a5d/download/museos_datosabiertos.csv'},
             {'categoria': 'salas-de-cine',
              'url': 'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/392ce1a8-ef11-4776-b280-6f1c7fae16ae/download/cine.csv'},
             {'categoria': 'bibliotecas',
              'url': 'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/01c6c048-dbeb-44e0-8efa-6944f73715d7/download/biblioteca_popular.csv'}]


class ArchivosCsv:

    @staticmethod
    def crear_directorios(categoria: str):
        NEW_DIR = fr'data\{DATE_DIRNAME}\{categoria}'
        PATH_NEW_DIR = os.path.join(BASE_DIR, NEW_DIR)
        try:
            os.makedirs(PATH_NEW_DIR)
            log.info('Se crean los directorios. Continúa el proceso de descarga.')
            return PATH_NEW_DIR
        except FileExistsError:
            log.info('Directorios ya existentes. Continúa el proceso de descarga.')
            return PATH_NEW_DIR

    def descarga_archivos(self):
        log.info('Ejecución ArchivosCsv.descarga_archivos()')
        with requests.Session() as s:
            for item in DATA_DICT:
                categoria = item['categoria']
                download_dir = self.crear_directorios(categoria)
                download = s.get(item['url'])
                decoded_content = download.content.decode('utf-8')
                cr = csv.reader(decoded_content.splitlines(), delimiter=',')
                my_list = list(cr)
                df = pd.DataFrame(data=my_list)
                try:
                    log.info(f'Comienza descarga de {categoria}')
                    df.to_csv(fr'{download_dir}\{categoria}-{FECHA_ACTUAL}.csv', index=False, header=False)
                    log.info(f'Archivo {categoria} descargado con éxito')
                except OSError as e:
                    log.info(f'Ocurrió un error {e}, reintentando descarga')
                    df.to_csv(fr'{download_dir}\{categoria}-{FECHA_ACTUAL}.csv', index=False, header=False)
                    log.info(f'Archivo {categoria} descargado con éxito')
                except Exception as e:
                    log.critical(f'Ocurrió un error {e}. No se pudo realizar la descarga de {categoria}')
                    raise Exception(f'Error: {e}')
    """
    def descarga_archivos(self):
        log.info('Ejecución ArchivosCsv.descarga_archivos()')
        with requests.Session() as s:
            for item in DATA_DICT:
                categoria = item['categoria']
                download = s.get(item['url'])
                decoded_content = download.content.decode('utf-8')
                cr = csv.reader(decoded_content.splitlines(), delimiter=',')
                my_list = list(cr)
                df = pd.DataFrame(data=my_list)
                NEW_DIR = fr'data\{DATE_DIRNAME}\{categoria}'
                PATH_NEW_DIR = os.path.join(BASE_DIR, NEW_DIR)
                try:
                    log.info(f'Comienza descarga de {categoria}')
                    df.to_csv(fr'{PATH_NEW_DIR}\{categoria}-{FECHA_ACTUAL}.csv', index=False, header=False)
                    log.info(f'Archivo {categoria} descargado con éxito')
                except OSError as e:
                    log.info(f'Ocurrió un error {e}, reintentando descarga')
                    os.makedirs(PATH_NEW_DIR)
                    df.to_csv(fr'{PATH_NEW_DIR}\{categoria}-{FECHA_ACTUAL}.csv', index=False, header=False)
                    log.info(f'Archivo {categoria} descargado con éxito')
                except Exception as e:
                    log.critical(f'Ocurrió un error {e}. No se pudo realizar la descarga de {categoria}')
                    raise Exception(f'Error: {e}')
    """

if __name__ == "__main__":
    print('Challenge de Data Analytics - Alkemy')

    ac = ArchivosCsv()

    ac.descarga_archivos()

    """
    with requests.Session() as s:
        for item in DATA_DICT:
            try:
                categoria = item['categoria']
                download = s.get(item['url'])
                decoded_content = download.content.decode('utf-8')
                cr = csv.reader(decoded_content.splitlines(), delimiter=',')
                my_list = list(cr)
                # for row in my_list:
                #     print(row)
                # print(my_list)
                df = pd.DataFrame(data=my_list)
                # print(df)
                NEW_DIR = fr'data\{DATE_DIRNAME}\{categoria}'
                PATH_NEW_DIR = os.path.join(BASE_DIR, NEW_DIR)
                df.to_csv(fr'{PATH_NEW_DIR}\{categoria}-{FECHA_ACTUAL}.csv', index=False, header=False)
            except OSError as e:
                categoria = item['categoria']
                NEW_DIR = fr'data\{DATE_DIRNAME}\{categoria}'
                PATH_NEW_DIR = os.path.join(BASE_DIR, NEW_DIR)
                os.makedirs(PATH_NEW_DIR)
                df.to_csv(fr'{PATH_NEW_DIR}\{categoria}-{FECHA_ACTUAL}.csv', index=False, header=False)
            except Exception as e:
                raise Exception(f'Error: {e}')
    """