CREATE TABLE IF NOT EXISTS nicolasmvinciguerra_coderhouse.dim_location
(
    locationid INTEGER
    description VARCHAR(100)
    latitude FLOAT
    longitude FLOAT
    city VARCHAR(100)
    country VARCHAR(100)
)
DISTSTYLE ALL --es una tabla de tipo SCD no muy extensa, recomendable que se replique en todos los nodos.
SORTKEY (locationid) --si bien es comun filtrar por ciudad o pais no se incluyen como sortkey ya que el locationid esta correlacionado con estos campos.
;

CREATE TABLE IF NOT EXISTS nicolasmvinciguerra_coderhouse.dim_parameter
(
    parameterid INTEGER
    displayname VARCHAR(100)
    description VARCHAR(100)
    preferredunit VARCHAR(100)
)
DISTSTYLE ALL --es una tabla de tipo SCD no muy extensa, recomendable que se replique en todos los nodos.
SORTKEY (parameterid) --si bien es comun filtrar por el nombre del parametro, su id lo identifica univocamente con lo cual no es necesario incluirlo como sortkey.
;

CREATE TABLE IF NOT EXISTS nicolasmvinciguerra_coderhouse.fact_measure
(
    locationid INTEGER
    parameterid INTEGER
    value FLOAT
    dateutc TIMESTAMP DISTKEY --es el campo mas comunmente utilizado para el filtrado
)
SORTKEY (dateutc,locationid,parameterid)
;