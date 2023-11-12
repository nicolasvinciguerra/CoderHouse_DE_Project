import requests
import pandas as pd
import json
from configparser import ConfigParser
from datetime import datetime, timedelta
import sqlalchemy as sa
from sqlalchemy.engine.url import URL

#Config file
config_dir = "config/config.ini"
config = ConfigParser()
config.read(config_dir)

#API config variables
api_key = config["openaq_api_credentials"]["api_key"]

def build_url(base_url, endpoint):
  endpoint_url = f"{base_url}/{endpoint}"
  return endpoint_url

def get_data(base_url, endpoint, headers, params=None):
    """
    Realiza una solicitud GET a una API para obtener datos

    :param base_url: la URL base de la API.
    :param endpoint: El endpoint de la API para obtener datos especificos.
    :param headers: Los headers para la solicitud GET.
    :param params: Parametros de la solicitud GET.
    :return: Los datos obtenidos de la API en formato JSON.
    """

    try:
        endpoint_url = build_url(base_url,endpoint)
        response = requests.get(endpoint_url, headers=headers, params=params)
        response.raise_for_status()
        try:
            data = response.json()["results"]
        except:
            raise ValueError("Formato de respuesta no esperado.")
        return data
    except requests.exceptions.RequestException as e:
        raise ValueError(f"La peticion ha fallado. Codigo del error: {e}")

def connect_to_db (config:ConfigParser, config_section):
    """
    Crea conexion a la base de datos
    a partir del archivo de configuracion.
    :param config: configparser con archivo de configuracion
    :param config_section: seccion dentro del archivo de configuracion para la coneccion
    :return conn: coneccion creada a la base de datos
    :return engine: engine de base de datos
    """

    ##Lee las config variables del archivo de configuracion
    drivername = config[config_section]["driver"]
    host = config[config_section]["host"]
    port = config[config_section]["port"]
    dbname = config[config_section]["database"]
    username = config[config_section]["username"]
    pwd = config[config_section]["password"]

    # construye sqlalchemy URL
    url = URL.create(
        drivername=drivername, # indicate driver and dialect will be used
        host=host, # Amazon Redshift host
        port=port, # Amazon Redshift port
        database=dbname, # Amazon Redshift database
        username=username, # Amazon Redshift username
        password=pwd # Amazon Redshift password
    )

    engine = sa.create_engine(url)
    conn = engine.connect()
    return conn, engine

def built_table(json_data):
    """
    Construye un DataFrame de pandas a partir de datos en formato JSON.

    :param json_data: Los datos en formato JSON.
    :return: Un DataFrame de pandas con los datos estructurados.
    """
    df = pd.json_normalize(json_data)
    return df

#extract API data
base_url = "https://api.openaq.org/v2/"
endpoint = "measurements"

headers = {
    "X-API-Key" : api_key,
    "accept": "application/json",
    "content-type": "application/json"
}

#Request locations



#Request 

start_date = datetime.utcnow() - timedelta(hours=24)
start_date = start_date.strftime("%Y-%m-%dT%H:00:00Z") #datos de las ultimas 24hs..

params = {
    "date_from" : start_date
}

json_api=get_data(base_url, endpoint, headers, params)
df_api = pd.json_normalize(json_api)

#Redshift connection
conn, engine = connect_to_db(config, "redshift_credentials")

#Redshift load
df_api.to_sql(
    name="openaq_measurements",
    con=conn,
    schema="nicolasmvinciguerra_coderhouse",
    if_exists="replace",
    method="multi",
    index=False,
    dtype={}
)