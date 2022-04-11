DROP DATABASE IF EXISTS data_analytics;

CREATE DATABASE data_analytics;

\c data_analytics;

-- 1era tabla: datos consolidados con la información normalizada
CREATE TABLE IF NOT EXISTS espaciosculturales (
    espacio_id          serial PRIMARY KEY,
    cod_localidad       VARCHAR(50),
    id_provincia        VARCHAR(2),
    id_departamento     VARCHAR(5),
    categoria           VARCHAR(50),
    provincia           VARCHAR(100),
    localidad           VARCHAR(100),
    nombre              VARCHAR(100),
    domicilio           VARCHAR(100),
    codigo_postal       VARCHAR(10),
    numero_telefono     VARCHAR(20),
    mail                VARCHAR(100),
    web                 VARCHAR(100),
    insert_date         DATE
);

-- 2da tabla: tabla con registros totalizados por categoria
CREATE TABLE IF NOT EXISTS totales_categoria (
    categoria           VARCHAR(20),
    cantidad            INTEGER,
    insert_date         DATE
);

-- 3era tabla: tabla con registros totalizados por fuente
CREATE TABLE IF NOT EXISTS totales_fuente (
    fuente              VARCHAR(100),
    cantidad            INTEGER,
    insert_date         DATE
);

-- 4ta tabla: tabla con registros totalizados por provincia y categoría
CREATE TABLE IF NOT EXISTS totales_provincia_y_cat (
    provincia           VARCHAR(100),
    categoria           VARCHAR(20),
    cantidad            INTEGER,
    insert_date         DATE
);

-- 5ta tabla: tabla con info de cines
CREATE TABLE IF NOT EXISTS salascine (
    provincia                   VARCHAR(100),
    cantidad_pantallas          INTEGER,
    cantidad_butacas            INTEGER,
    cantidad_espacios_INCAA     INTEGER,
    insert_date                 DATE
);