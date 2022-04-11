DROP DATABASE IF EXISTS data_analytics;

CREATE DATABASE data_analytics;

\c data_analytics;

[cod_localidad, id_provincia, id_departamento,
categoria, provincia, localidad, nombre,
domicilio, codigo_postal, numero_telefono,
mail, web]

-- 1era tabla: todos los datos con la información normalizada
CREATE TABLE IF NOT EXISTS espaciosculturales (
    espacio_id          serial PRIMARY KEY,
    cod_localidad       INTEGER,
    id_provincia        VARCHAR(2),
    id_departamento     VARCHAR(5),
    categoria           VARCHAR(50),
    provincia           VARCHAR(50),
    localidad           VARCHAR(50),
    nombre              VARCHAR(100),
    domicilio           VARCHAR(100),
    codigo_postal       VARCHAR(10),
    numero_telefono     VARCHAR(20),
    mail                VARCHAR(100),
    web                 VARCHAR(100),
    insert_timestamp    TIMESTAMP
);

-- 2da tabla: tabla con registros totalizados etc
CREATE TABLE IF NOT EXISTS registrostotales (
    totales_categoria               INTEGER,
    totales_fuente                  INTEGER,  -- por link?
    totales_provincia_categoria     INTEGER,
    insert_timestamp                TIMESTAMP
)

-- 3era tabla: tabla con info de cines
CREATE TABLE IF NOT EXISTS salascine (
    id_provincia                serial PRIMARY KEY,
    cantidad_pantallas          INTEGER,
    cantidad_butacas            INTEGER,
    cantidad_espacios_INCAA     INTEGER, -- buscar "Espacio incaa" en columna nombre
    insert_timestamp            TIMESTAMP
)