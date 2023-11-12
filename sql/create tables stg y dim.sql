--STAR SCHEME DIMENSIONAL MODEL - CREATE TABLES
DROP TABLE IF EXISTS dim_location;
CREATE TABLE IF NOT EXISTS nicolasmvinciguerra_coderhouse.dim_location (
    id INT,
    city VARCHAR(256),
    name VARCHAR(256),
    country_code VARCHAR(2),
    country_name VARCHAR(50),
    sources VARCHAR(256),
    ismobile BOOL,
    isanalysis BOOL,
    sensortype VARCHAR(256),
    coordinates_latitude FLOAT,
    coordinates_longitude FLOAT,
    updated_at timestamp without time zone
    )
DISTSTYLE ALL --es una tabla de tipo SCD no muy extensa, recomendable que se replique en todos los nodos.
SORTKEY (id); --si bien es comun filtrar por ciudad o pais no se incluyen como sortkey ya que el locationid esta correlacionado con estos campos.

DROP TABLE IF EXISTS dim_parameter;
CREATE TABLE IF NOT EXISTS nicolasmvinciguerra_coderhouse.dim_parameter (
    id INT,
    name VARCHAR(256),
    displayName	VARCHAR(256),
    description	VARCHAR(256),
    preferredUnit VARCHAR(256),
    updated_at timestamp without time zone
    )
DISTSTYLE ALL --es una tabla de tipo SCD no muy extensa, recomendable que se replique en todos los nodos.
SORTKEY (id); --si bien es comun filtrar por el nombre del parametro, su id lo identifica univocamente con lo cual no es necesario incluirlo como sortkey.

DROP TABLE IF EXISTS fact_measure;
CREATE TABLE IF NOT EXISTS nicolasmvinciguerra_coderhouse.fact_measure (
    location_id INTEGER,
    parameter_code VARCHAR(20),
    value FLOAT,
    date_local TIMESTAMP,
    date_utc TIMESTAMP DISTKEY, --es el campo mas comunmente utilizado para el filtrado
    updated_at timestamp without time zone
    )
SORTKEY (date_utc,location_id,parameter_code);


--STAGING TABLES
DROP TABLE IF EXISTS stg_countries;
CREATE TABLE  nicolasmvinciguerra_coderhouse.stg_countries (
    code VARCHAR(2),
    name VARCHAR(50)
    )
DISTSTYLE EVEN;

DROP TABLE IF EXISTS stg_locations;
CREATE TABLE  nicolasmvinciguerra_coderhouse.stg_locations (
    id INT,
    city VARCHAR(256),
    name VARCHAR(256),
    entity VARCHAR(256),
    country VARCHAR(2),
    sources VARCHAR(256),
    ismobile BOOL,
    isanalysis BOOL,
    sensortype VARCHAR(256),
    lastupdated TIMESTAMP,
    firstupdated TIMESTAMP,
    coordinates_latitude FLOAT,
    coordinates_longitude FLOAT
    )
DISTSTYLE EVEN;

DROP TABLE IF EXISTS stg_parameters;
CREATE TABLE  nicolasmvinciguerra_coderhouse.stg_parameters (
    id INT,
    name VARCHAR(256),
    displayName	VARCHAR(256),
    description	VARCHAR(256),
    preferredUnit VARCHAR(256)
    )
DISTSTYLE EVEN;

DROP TABLE IF EXISTS stg_measurements;
CREATE TABLE  nicolasmvinciguerra_coderhouse.stg_measurements (
    locationId INT,
    location VARCHAR(256),
    parameter VARCHAR(20),
    value FLOAT,
    unit VARCHAR(256),
    country VARCHAR(2),
    city VARCHAR(256),
    isMobile BOOL,
    isAnalysis BOOL,
    entity	VARCHAR(256),
    sensorType VARCHAR(256),
    date_utc TIMESTAMP,
    date_local TIMESTAMP,
    coordinates_latitude FLOAT,
    coordinates_longitude FLOAT
    )
DISTSTYLE EVEN;
