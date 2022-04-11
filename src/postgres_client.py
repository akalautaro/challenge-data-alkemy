import os
import pandas as pd
import datetime
import locale
from logger_base import log
from decouple import config
import psycopg2
from sqlalchemy import create_engine
import numpy as np


class PostgresClient:
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
            log.info(f'Conectado con éxito a {self.db_name}')
            log.info('Engine creado con éxito')
        except psycopg2.Error as e:
            print(f'Error de conexión {e}')

    def carga_info_consolidada(self, data_consolidada: pd.DataFrame):
        log.info('Cargando información consolidada a la base de datos')
        data_consolidada['espacio_id'] = data_consolidada.index
        data_consolidada.reset_index(drop=True, inplace=True)
        data_consolidada.set_index('espacio_id', inplace=True)

        # print(data_consolidada)
        # print(data_consolidada.columns)

        data_consolidada['insert_date'] = datetime.date.today().strftime("%Y-%m-%d")
        data_consolidada.columns = ['cod_localidad', 'id_provincia', 'id_departamento', 'categoria', 'provincia',
                                    'localidad', 'nombre', 'domicilio', 'codigo_postal', 'numero_telefono', 'mail',
                                    'web', 'insert_date']

        log.info('Transformando data_consolidada')
        try:
            data_consolidada.to_sql('espaciosculturales', self.engine, if_exists='replace', chunksize=100)
            log.info('Información consolidada cargada con éxito')
        except Exception as e:
            log.error(f'Ocurrió un error subiendos los datos: {e}')

    def carga_datos_totalizados(self, data: pd.DataFrame):
        log.info('Carga de datos a tablas totalizadoras'.center(100, '-'))

        cat_y_prov = data.groupby(['Provincia', 'Categoria']).count()
        cat = data.groupby(['Categoria']).count()
        fuente = data.groupby(['Fuente']).count()

        print('-------------')
        print(cat_y_prov)
        print('-------------')
        print(cat)
        print('-------------')
        print(fuente)
        print('-------------')

        cat_y_prov.columns = cat_y_prov.columns.get_level_values(0)
        cat.columns = cat.columns.get_level_values(0)
        fuente.columns = fuente.columns.get_level_values(0)

        cat_y_prov['insert_date'] = datetime.date.today().strftime("%Y-%m-%d")
        cat['insert_date'] = datetime.date.today().strftime("%Y-%m-%d")
        fuente['insert_date'] = datetime.date.today().strftime("%Y-%m-%d")

        cat_y_prov.columns = ['cantidad', 'insert_date']
        cat.columns = ['cantidad', 'insert_date']
        fuente.columns = ['cantidad', 'insert_date']

        log.info('Cargando data')
        try:
            cat_y_prov.to_sql('totales_provincia_y_cat', self.engine, if_exists='replace', chunksize=100)
            cat.to_sql('totales_categoria', self.engine, if_exists='replace', chunksize=100)
            fuente.to_sql('totales_fuente', self.engine, if_exists='replace', chunksize=100)
            log.info('Datos agrupados por provincia y categoria cargados con éxitos')
        except Exception as e:
            log.error(f'Ocurrió un error subiendos los datos: {e}')

    def data_cines_to_postgres(self, data: pd.DataFrame):
        log.info('data_cines_to_postgres'.center(100, '-'))

        data['Pantallas'] = data['Pantallas'].astype(int)
        data['Butacas'] = data['Butacas'].astype(int)

        if data['espacio_INCAA'].empty:
            data['espacio_INCAA'] = 0
        else:
            data['espacio_INCAA'] = 1

        df_agrupado = data.groupby('Provincia').agg({'Pantallas': [np.sum],
                                                     'Butacas': [np.sum],
                                                     'espacio_INCAA': [np.sum]})
        print('-------------')
        print(df_agrupado)
        print('-------------')

        df_agrupado.columns = df_agrupado.columns.get_level_values(0)

        print('-------------')
        print(df_agrupado)
        print('-------------')

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
