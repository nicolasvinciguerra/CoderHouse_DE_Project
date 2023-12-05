# CoderHouse Data Engineering project - OpenAQ

## Project Overview
En este proyecto se extraen datos sobre calidad del aire de la plataforma abierta "OpenAQ" mediante su API publica, se transforman utilizando Pandas y se cargan en un data warehouse en Redshift.
El modelo dimensional en Redshift para la carga de datos utilizado es un modelo estrella, con una unica tabla de hechos que contiene valores de mediciones del aire realizadas y dos tablas de dimensiones denormalizadas con localizaciones y parametros de las mediciones.
El campo de fecha se incluye dentro de la tabla de hechos de forma denormalizada.

Debido a que las localizaciones y los parametros que se miden no cambian con frecuecia, se optó por utilizar SCD type 1 para su actualizacion (se sobre escriben los registros en caso de cambio), y el estilo de distribucion en Redshift es ALL, debido a que son tablas no extensas muy poco cambiantes y que se utilizan de forma muy frecuente para JOINS con la tabla de hechos, con lo cual es recomendable que se repliquen en todos los nodos. Ademas se incluyen como SORTKEY sus respectivos ids.
En Redshift la tabla de hechos utiliza SORTKEY el campo de fecha y las ids a las tablas de dim_location y dim_parameter para obtener una mejor performance; es frecuente que se utilicen estos campos para filtrar los datos y hacer joins.


## Sobre OpenAQ
OpenAQ is the largest open-source air quality data platform, aggregating and harmonizing historical and real-time air quality data from diverse sources from across the globe.

OpenAQ API Documentation Site: https://docs.openaq.org/docs

## Usage Guide
### Ejecucion con Docker
Se ejecuta "docker compose up -d" en el directorio del proyecto. Para ello docker debe estar corriendo.
Por defecto el web server es accesible por http://localhost:8080/ con usr y password "airflow", como se detalla en el archivo "docker-compose.yaml".
Para la ejecucion de los dags se debe generar la coneccion en Airflow, segun los parametros para Redshift del archivo "config.ini".

### Ejecucion como script
Previo a ejecutar main.py, se deben ejecutar 2 scripts SQL:
1- Ejecutar el script SQL "creates.sql" del directorio '/dags/sql'. El mismo contiene la query sql para crear las tablas de staging (empiezan con el prefijo 'stg') y es donde se almacen temporalmente los datos extraidos desde la API de OpenAQ antes de su insersion en el modelo dimensional, como asi tambien las tablas del modelo estrella del DW: dos de dimensiones (con el prefijo 'dim'), y una de hechos (con el prefijo 'fact').
2- Ejecutar el script SQL "stored_procedures.sql" del directorio '/dags/sql'. El mismo contiene 4 procedimientos almacenados que son ejecutados desde el flujo del programa para actualizar las tablas del modelo dimensional a partir de los datos de las tablas de staging en un determinando momento.

## Libraries
Este proyecto utiliza las siguientes librerias:
- sqlalchemy-redshift
- redshift_connector
- configparser
- pandas
- datetime
- logging
- python formatted with black (python3 -m black)