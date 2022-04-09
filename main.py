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
LISTA_COLUMNAS = ['cod_localidad', 'id_provincia', 'id_departamento', 'categoria', 'provincia', 'localidad',
                  'nombre', 'domicilio', 'codigo_postal', 'numero_telefono', 'mail', 'web']
dataframe_completo = pd.DataFrame(columns=LISTA_COLUMNAS)  # inicializo un dataframe vacío


class ArchivosCsv:

    conjuntos = []

    @staticmethod
    def crear_directorios(categoria: str):
        new_dir = fr'data\{DATE_DIRNAME}\{categoria}'
        path_new_dir = os.path.join(BASE_DIR, new_dir)
        try:
            os.makedirs(path_new_dir)
            log.info('Se crean los directorios. Continúa el proceso de descarga.')
            return path_new_dir
        except FileExistsError:
            log.info('Directorios ya existentes. Continúa el proceso de descarga.')
            return path_new_dir

    def transformar_data(self, dataframe_parcial: pd.DataFrame, categoria: str):
        df_transformado = pd.DataFrame()
        df_transformado.insert(0, 'cod_localidad', dataframe_parcial['Cod_Loc'])
        df_transformado.insert(1, 'id_provincia', dataframe_parcial['IdProvincia'])
        df_transformado.insert(2, 'id_departamento', dataframe_parcial['IdDepartamento'])
        df_transformado.insert(3, 'categoria', dataframe_parcial['Categoría'])
        df_transformado.insert(4, 'provincia', dataframe_parcial['Provincia'])
        df_transformado.insert(5, 'localidad', dataframe_parcial['Localidad'])
        df_transformado.insert(6, 'nombre', dataframe_parcial['Nombre'])
        if categoria == 'bibliotecas':
            df_transformado.insert(7, 'domicilio', dataframe_parcial['Domicilio'])
        else:
            df_transformado.insert(7, 'domicilio', dataframe_parcial['Dirección'])
        df_transformado.insert(8, 'codigo_postal', dataframe_parcial['CP'])
        df_transformado.insert(9, 'numero_telefono', dataframe_parcial['Teléfono'])
        df_transformado.insert(10, 'mail', dataframe_parcial['Mail'])
        df_transformado.insert(11, 'web', dataframe_parcial['Web'])
        # dataframe_completo.append(df_transformado)
        self.conjuntos.append(df_transformado)

    def transformar_data_museos(self, dataframe_parcial: pd.DataFrame):
        df_transformado = pd.DataFrame()
        df_transformado.insert(0, 'cod_localidad', dataframe_parcial['Cod_Loc'])
        df_transformado.insert(1, 'id_provincia', dataframe_parcial['IdProvincia'])
        df_transformado.insert(2, 'id_departamento', dataframe_parcial['IdDepartamento'])
        df_transformado.insert(3, 'categoria', dataframe_parcial['categoria'])
        df_transformado.insert(4, 'provincia', dataframe_parcial['provincia'])
        df_transformado.insert(5, 'localidad', dataframe_parcial['localidad'])
        df_transformado.insert(6, 'nombre', dataframe_parcial['nombre'])
        df_transformado.insert(7, 'domicilio', dataframe_parcial['direccion'])
        df_transformado.insert(8, 'codigo_postal', dataframe_parcial['CP'])
        df_transformado.insert(9, 'numero_telefono', dataframe_parcial['telefono'])
        df_transformado.insert(10, 'mail', dataframe_parcial['Mail'])
        df_transformado.insert(11, 'web', dataframe_parcial['Web'])
        # dataframe_completo.append(df_transformado)
        self.conjuntos.append(df_transformado)
        # print(dataframe_completo.head)

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
                df = pd.DataFrame(data=my_list, columns=my_list[0])
                #df.columns = df.iloc[0]
                df = df.drop(df.index[0])
                try:
                    log.info(f'Comienza descarga de {categoria}')
                    df.to_csv(fr'{download_dir}\{categoria}-{FECHA_ACTUAL}.csv', index=False)
                    # df.to_csv(fr'{download_dir}\{categoria}-{FECHA_ACTUAL}.csv', index=False, header=False)
                    log.info(f'Archivo {categoria} descargado con éxito')
                except OSError as e:
                    log.info(f'Ocurrió un error {e}, reintentando descarga')
                    df.to_csv(fr'{download_dir}\{categoria}-{FECHA_ACTUAL}.csv', index=False)
                    # df.to_csv(fr'{download_dir}\{categoria}-{FECHA_ACTUAL}.csv', index=False, header=False)
                    log.info(f'Archivo {categoria} descargado con éxito')
                except Exception as e:
                    log.critical(f'Ocurrió un error {e}. No se pudo realizar la descarga de {categoria}')
                    raise Exception(f'Error: {e}')
                finally:
                    if categoria == 'museos':
                        log.info('Se transforma data de museos')
                        self.transformar_data_museos(df)
                    elif categoria == 'salas-de-cine' or categoria == 'bibliotecas':
                        log.info(f'Se transforma data de {categoria}')
                        self.transformar_data(df, categoria=categoria)
                    # dataframe_completo a base de datos
        print(self.conjuntos)
        conjunto_completo = pd.concat(self.conjuntos)
        conjunto_completo.to_csv(f'./data/Conjunto-datos-completo-{FECHA_ACTUAL}.csv', index=False)


if __name__ == "__main__":
    log.info('Challenge de Data Analytics - Alkemy')

    ac = ArchivosCsv()

    ac.descarga_archivos()
