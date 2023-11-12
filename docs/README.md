# HoderHouse Data Engineering project - OpenAQ

## Project Overview
En este proyecto se extraen datos sobre calidad del aire de la plataforma abierta "OpenAQ" mediante su API publica, se transforman utilizando Pandas y se cargan en un data warehouse en Redshift.
El modelo dimensional en Redshift para la carga de datos utilizado es un modelo estrella, con una unica tabla de hechos que contiene valores de mediciones del aire realizadas y dos tablas de dimensiones denormalizadas con localizaciones y parametros de las mediciones.
El campo de fecha se incluye dentro de la tabla de hechos de forma denormalizada.
Esquema del DW:

Debido a que las localizaciones y los parametros que se miden no cambian con frecuecia, se optó por utilizar SCD type 1 para su actualizacion (se sobre escriben los registros en caso de cambio), y el estilo de distribucion en Redshift es ALL, debido a que son tablas no extensas muy poco cambiantes y que se utilizan de forma muy frecuente para JOINS con la tabla de hechos, con lo cual es recomendable que se repliquen en todos los nodos. Ademas se incluyen como SORTKEY sus respectivos ids.
En Redshift la tabla de hechos utiliza SORTKEY el campo de fecha y las ids a las tablas de dim_location y dim_parameter para obtener una mejor performance; es frecuente que se utilicen estos campos para filtrar los datos y hacer joins.


## Sobre OpenAQ
OpenAQ is the largest open-source air quality data platform, aggregating and harmonizing historical and real-time air quality data from diverse sources from across the globe.

OpenAQ API Documentation Site: https://docs.openaq.org/docs

## Installation Instructions
Provide clear steps for installing your project. Include any prerequisites, package managers, or commands required.

pip3 install sqlalchemy-redshift
pip3 install redshift_connector

## Usage Guide
Walk users through how to use your project. Include code examples, API endpoints, and any configuration they need to know.

