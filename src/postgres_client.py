import pandas as pd
import datetime
from logger_base import log
from decouple import config
import psycopg2
from sqlalchemy import create_engine
import numpy as np


class PostgresClient:
    """ Esta clase se encarga de la conexión a la base de datos PostgreSQL, como también de la transformación de los
    datos provenientes de distintos dataframes para su posterior carga en la base de datos.
    """
    db_user = ""
    db_pwd = ""
    db_host = ""
    db_port = 5432
    db_dbn = ""
    conn: psycopg2

    def __init__(self, user="", passwd="", dbn="", host="", port=5458):
        self.db_host = host or config('DB_HOST', default='localhost')
        self.db_port = port or config('DB_PORT', cast=int, default=5458)
        self.db_name = dbn or config('DB_NAME')
        self.db_user = user or config('DB_USER')
        self.db_password = passwd or config('DB_PASSWORD')
        self.database_url = config('DATABASE_URL')
        self.engine = create_engine(self.database_url)

        try:
            conn_str = f"host={self.db_host} port={self.db_port} dbname={self.db_name} user={self.db_user} password={self.db_password}"
            self.conn = psycopg2.connect(conn_str)
            self.conn.autocommit = True
            log.info('Engine creado con éxito')
            log.info(f'Conectado con éxito a {self.db_name}')
        except psycopg2.Error as e:
            log.error(f'Error de conexión {e}')

    def carga_info_consolidada(self, data_consolidada: pd.DataFrame):
        """ Función dedicada a transformar y cargar los datos provenientes de un dataframe que contiene la info
         correspondiente a las 3 categorías de espacios en la base de datos.

        Args:
            data_consolidada (pd.DataFrame): contiene info de interés para las categorías museos, salas de cine y
            bibliotecas.
        """
        log.info('Cargando información consolidada a la base de datos')
        data_consolidada['espacio_id'] = data_consolidada.index
        data_consolidada.reset_index(drop=True, inplace=True)
        data_consolidada.set_index('espacio_id', inplace=True)

        data_consolidada['insert_date'] = datetime.date.today().strftime("%Y-%m-%d")
        data_consolidada.columns = ['cod_localidad', 'id_provincia', 'id_departamento', 'categoria', 'provincia',
                                    'localidad', 'nombre', 'domicilio', 'codigo_postal', 'numero_telefono', 'mail',
                                    'web', 'insert_date']

        log.info('Carga data_consolidada')
        try:
            data_consolidada.to_sql('espaciosculturales', self.engine, if_exists='replace', chunksize=100)
            log.info('Información consolidada cargada con éxito')
        except Exception as e:
            log.error(f'Ocurrió un error subiendos los datos: {e}')

    def carga_datos_totalizados(self, data: pd.DataFrame):
        """ Función que transforma agrupando por distintas columnas y aplicando operaciones sobre las mismas y carga
        los datos en la base de datos, en las tablas dedicadas a totalizar por categoría, fuente y categoria/provincia.

        Args:
            data (pd.DataFrame): DataFrame de pandas con información de las categorías, fuente y provincia.
        """
        log.info('Carga de datos a tablas totalizadoras')

        cat_y_prov = data.groupby(['provincia', 'categoria']).count()
        cat = data.groupby(['categoria']).agg('count').reset_index()
        fuente = data.groupby(['fuente']).agg('count').reset_index()

        cat_y_prov.columns = cat_y_prov.columns.get_level_values(0)
        cat.columns = cat.columns.get_level_values(0)
        fuente.columns = fuente.columns.get_level_values(0)

        df_fuente = pd.DataFrame()
        df_fuente['fuente'] = fuente['fuente']
        df_fuente['cantidad'] = fuente['provincia']

        df_cat = pd.DataFrame()
        df_cat['categoria'] = cat['categoria']
        df_cat['cantidad'] = cat['provincia']

        cat_y_prov['insert_date'] = datetime.date.today().strftime("%Y-%m-%d")
        df_cat['insert_date'] = datetime.date.today().strftime("%Y-%m-%d")
        df_fuente['insert_date'] = datetime.date.today().strftime("%Y-%m-%d")

        cat_y_prov.columns = ['cantidad', 'insert_date']
        df_cat.columns = ['categoria', 'cantidad', 'insert_date']
        df_fuente.columns = ['fuente', 'cantidad', 'insert_date']

        dataframes = [cat_y_prov, df_cat, df_fuente]
        tables = ['totales_provincia_y_cat', 'totales_categoria', 'totales_fuente']
        add_index = [True, False, False]
        dataframes_a_sql = zip(dataframes, tables, add_index)

        log.info('Comienza la carga de datos')
        for info_df in dataframes_a_sql:
            try:
                info_df[0].to_sql(info_df[1], self.engine, if_exists='replace', index=info_df[2])
                log.info(f'Información insertada a {info_df[1]} con éxito')
            except Exception as e:
                log.error(f'Ocurrió un error insertando la data a {info_df[1]}. Error: {e}')
            else:
                log.info('Toda la información fue insertada con éxito')

    def data_cines_to_postgres(self, data: pd.DataFrame):
        """ Función que transforma agrupando por Provincia y aplicando operaciones sobre las columnas y carga los datos
        de los cines a la base de datos, en una tabla dedicada exclusivo a estos espacios.

        Args:
            data (pd.DataFrame): DataFrame de pandas con información de las pantallas, butacas y booleano que indica
            si el cine es considerado espacio_INCAA o no.
        """
        log.info('data_cines_to_postgres')

        data['Pantallas'] = data['Pantallas'].astype(int)
        data['Butacas'] = data['Butacas'].astype(int)

        if data['espacio_INCAA'].empty:
            data['espacio_INCAA'] = 0
        else:
            data['espacio_INCAA'] = 1

        df_agrupado = data.groupby('Provincia').agg({'Pantallas': [np.sum],
                                                     'Butacas': [np.sum],
                                                     'espacio_INCAA': [np.sum]})

        df_agrupado.columns = df_agrupado.columns.get_level_values(0)

        df_agrupado['insert_date'] = datetime.date.today().strftime("%Y-%m-%d")
        df_agrupado.columns = ['cantidad_pantallas', 'cantidad_butacas', 'cantidad_espacios_INCAA', 'insert_date']

        log.info('Cargando data')
        try:
            df_agrupado.to_sql('salascine', self.engine, if_exists='replace', chunksize=100)
            log.info('Datos de "salas de cine" cargados con éxitos')
        except Exception as e:
            log.error(f'Ocurrió un error subiendos los datos: {e}')


if __name__ == "__main__":

    pgclient = PostgresClient()
