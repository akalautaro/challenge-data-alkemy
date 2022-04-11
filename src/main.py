import os
import requests
import csv
import pandas as pd
import datetime
import locale
from logger_base import log
from decouple import config
from ast import literal_eval

# Importo módulo con la conexión al cliente PostgreSQL
from postgres_client import PostgresClient

pgclient = PostgresClient()

# Configuraciones iniciales y algunas constantes
locale.setlocale(locale.LC_TIME, '')  # Esto para que el nombre de los meses sea en español
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
FECHA_ACTUAL = datetime.date.today().strftime('%d-%m-%Y')
DATE_DIRNAME = datetime.date.today().strftime('%Y-%B')

DATA_DICT = config('DATA_DICT', cast=literal_eval)


class ArchivosCsv:
    conjuntos = []
    conjunto_tabla_totales = []

    @staticmethod
    def crear_directorios(categoria: str):
        """ Crea los directorios y subdirectorios para almacenar los archivos csv descargados y devuelve la ruta del
        mismo.

        Args:
            categoria (str): en base a la categoría crea el sub-directorio correspondiente

        Returns:
            path_new_dir (str): path donde se descargará el archivo csv para cada categoría
        """
        new_dir = fr'data\{DATE_DIRNAME}\{categoria}'
        path_new_dir = os.path.join(BASE_DIR, new_dir)
        try:
            os.makedirs(path_new_dir)
            log.info('Se crean los directorios. Continúa el proceso de descarga.')
            return path_new_dir
        except FileExistsError:
            log.info('Directorios ya existentes. Continúa el proceso de descarga.')
            return path_new_dir

    def extract_data(self, dataframe_parcial: pd.DataFrame, categoria: str):
        """ Función que extrae los datos de interés de los archivos para las categorías bibliotecas y salas de cine. Al
        DataFrame resultante lo almacena en la lista conjuntos, que luego se encargará de concatenar los N DataFrames

        Args:
            dataframe_parcial (pd.DataFrame): dataframe con la info del archivo .csv
            categoria (str): tipo de institución (museo, sala de cine, biblioteca)
        """
        timestamp = datetime.datetime.now()
        df_transformado = pd.DataFrame()
        df_transformado.insert(0, 'cod_localidad', dataframe_parcial['Cod_Loc'].astype(str))
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
        if categoria == 'bibliotecas':
            df_transformado.insert(9, 'numero_telefono', dataframe_parcial['Cod_tel']+dataframe_parcial['Teléfono'])
        else:
            df_transformado.insert(9, 'numero_telefono', dataframe_parcial['cod_area']+dataframe_parcial['Teléfono'])
        df_transformado.insert(10, 'mail', dataframe_parcial['Mail'])
        df_transformado.insert(11, 'web', dataframe_parcial['Web'])

        self.conjuntos.append(df_transformado)

    def extract_data_museos(self, dataframe_parcial: pd.DataFrame):
        """ Función que extrae los datos de interés de los archivos para la categoría museos. Al DataFrame resultante
        lo almacena en la lista conjuntos, que luego se encargará de concatenar los N DataFrames.

        Args:
            dataframe_parcial (pd.DataFrame): dataframe con la info del archivo .csv
        """
        df_transformado = pd.DataFrame()
        df_transformado.insert(0, 'cod_localidad', dataframe_parcial['Cod_Loc'].astype(str))
        df_transformado.insert(1, 'id_provincia', dataframe_parcial['IdProvincia'])
        df_transformado.insert(2, 'id_departamento', dataframe_parcial['IdDepartamento'])
        df_transformado.insert(3, 'categoria', dataframe_parcial['categoria'])
        df_transformado.insert(4, 'provincia', dataframe_parcial['provincia'])
        df_transformado.insert(5, 'localidad', dataframe_parcial['localidad'])
        df_transformado.insert(6, 'nombre', dataframe_parcial['nombre'])
        df_transformado.insert(7, 'domicilio', dataframe_parcial['direccion'])
        df_transformado.insert(8, 'codigo_postal', dataframe_parcial['CP'])
        df_transformado.insert(9, 'numero_telefono', dataframe_parcial['cod_area']+dataframe_parcial['telefono'])
        df_transformado.insert(10, 'mail', dataframe_parcial['Mail'])
        df_transformado.insert(11, 'web', dataframe_parcial['Web'])
        self.conjuntos.append(df_transformado)

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
                df = df.drop(df.index[0])
                try:
                    log.info(f'Comienza descarga de {categoria}')
                    df.to_csv(fr'{download_dir}\{categoria}-{FECHA_ACTUAL}.csv', index=False)
                    log.info(f'Archivo {categoria} descargado con éxito')
                except OSError as e:
                    log.info(f'Ocurrió un error {e}, reintentando descarga')
                    df.to_csv(fr'{download_dir}\{categoria}-{FECHA_ACTUAL}.csv', index=False)
                    log.info(f'Archivo {categoria} descargado con éxito')
                except Exception as e:
                    log.critical(f'Ocurrió un error {e}. No se pudo realizar la descarga de {categoria}')
                    raise Exception(f'Error: {e}')
                finally:
                    log.info('Comienza la extracción de los datos')
                    if categoria == 'museos':
                        log.info('Se extrae data de museos')
                        self.extract_data_museos(df)
                        self.tabla_totales(df, categoria)
                    elif categoria == 'salas-de-cine' or categoria == 'bibliotecas':
                        log.info(f'Se extrae data de {categoria}')
                        if categoria == 'salas-de-cine':
                            self.extract_data(df, categoria=categoria)
                            pgclient.data_cines_to_postgres(df)
                            self.tabla_totales(df, categoria)
                        else:
                            self.extract_data(df, categoria=categoria)
                            self.tabla_totales(df, categoria)

        # conjunto_completo = pd.concat(self.conjuntos)
        conjunto_completo = self.consolida_info(self.conjuntos)
        # print(conjunto_completo)
        pgclient.carga_info_consolidada(conjunto_completo)
        conjunto_completo.to_csv(f'./data/Conjunto-datos-completo-{FECHA_ACTUAL}.csv', index=False)

        conjunto_tabla_totales = self.consolida_info(self.conjunto_tabla_totales)
        # print(conjunto_tabla_totales)
        pgclient.carga_datos_totalizados(conjunto_tabla_totales)

    def tabla_totales(self, dataframe_parcial: pd.DataFrame, categoria: str):
        """ Función de apoyo que uso para separar la info y popular las tablas que totalizan por fuente, categoría y
        provincia, sólo con los datos que me interesan.

        Args:
            dataframe_parcial (pd.DataFrame): dataframe con la info del archivo .csv
            categoria (str): tipo de institución (museo, sala de cine, biblioteca)
        """
        df_tabla_totales = pd.DataFrame()

        if categoria == 'museos':
            df_tabla_totales.insert(0, 'Provincia', dataframe_parcial['provincia'])
            df_tabla_totales.insert(1, 'Categoria', dataframe_parcial['categoria'])
            df_tabla_totales.insert(2, 'Fuente', dataframe_parcial['fuente'])
        else:
            df_tabla_totales.insert(0, 'Provincia', dataframe_parcial['Provincia'])
            df_tabla_totales.insert(1, 'Categoria', dataframe_parcial['Categoría'])
            df_tabla_totales.insert(2, 'Fuente', dataframe_parcial['Fuente'])

        self.conjunto_tabla_totales.append(df_tabla_totales)

    def consolida_info(self, lista_dataframes: list):
        """ Función que junta la información en forma de dataframe proveniente de las distintas fuentes y las devuelve
        en un único dataframe.

        Args:
            lista_dataframes (:obj:`list` of :obj:`pd.DataFrame`): lista con los DataFrames resultantes de la extracción
            de data.

        Returns:
            info_consolidada (pd.DataFrame): dataframe que contiene la info consolidada de las distintas fuentes para
            ser procesado.
        """
        info_consolidada = pd.concat(lista_dataframes)
        return info_consolidada


if __name__ == "__main__":
    log.info('Challenge de Data Analytics - Alkemy')

    ac = ArchivosCsv()

    ac.descarga_archivos()
