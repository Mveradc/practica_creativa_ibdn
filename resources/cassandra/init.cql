-- Keyspace
CREATE KEYSPACE IF NOT EXISTS agile_data_science
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE agile_data_science;

-- Tabla de distancias entre aeropuertos
-- Clave de partición: (Origin, Dest) — el patrón de acceso es siempre por estos dos campos
CREATE TABLE IF NOT EXISTS origin_dest_distances (
    origin  TEXT,
    dest    TEXT,
    distance DOUBLE,
    PRIMARY KEY ((origin, dest))
);

-- Tabla de predicciones
-- Clave de partición: uuid — Flask siempre consulta por UUID
CREATE TABLE IF NOT EXISTS flight_delay_ml_response (
    uuid        TEXT PRIMARY KEY,
    origin      TEXT,
    dest        TEXT,
    carrier     TEXT,
    flight_date TEXT,
    dep_delay   DOUBLE,
    distance    DOUBLE,
    day_of_week INT,
    day_of_year INT,
    day_of_month INT,
    timestamp   TEXT,
    prediction  DOUBLE
);
