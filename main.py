from configparser import ConfigParser
import pandas as pd
import datetime
from utils import *
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Config file
config_dir = "config/config.ini"
config = ConfigParser()
config.read(config_dir)
logging.info("Archivo de configuracion leido correctamente.")

# API config variables
api_key = config["openaq_api_credentials"]["api_key"]
base_url = "https://api.openaq.org/v2"
headers = {
    "X-API-Key": api_key,
    "accept": "application/json",
    "content-type": "application/json",
}

# Create Redshift Conn based on config file
engine = connect_to_db(config, "redshift_credentials")
schema = config["redshift_credentials"]["schema"]

# Get locations from OpenAQ API
endpoint = "locations"
params = {"limit": 1000}
json_api = get_data(base_url, endpoint, headers, params=params)
df_api = pd.json_normalize(data=json_api, sep="_")
df_api.drop(
    ["parameters", "measurements", "manufacturers", "bounds"], axis=1, inplace=True
)

with engine.connect() as conn, conn.begin():
    conn.execute("TRUNCATE TABLE stg_locations;")
    load_to_sql(
        df=df_api,
        table_name="stg_locations",
        engine=engine,
        if_exists="append",
    )
    query = f"CALL {schema}.petl_load_dim_location();"
    logging.info(f"Ejecutando query {query}...")
    conn.execute(query)
    logging.info(f"Se ejecuto correctamente la query: {query}")

# Get countries from OpenAQ API
endpoint = "countries"
params = {"limit": 1000}
json_api = get_data(base_url, endpoint, headers, params=params)
df_api = pd.json_normalize(data=json_api, sep="_")

with engine.connect() as conn, conn.begin():
    conn.execute("TRUNCATE TABLE stg_countries;")
    load_to_sql(
        df=df_api[["code", "name"]],
        table_name="stg_countries",
        engine=engine,
        if_exists="append",
    )
    query = f"CALL {schema}.petl_update_country_dim_location();"
    logging.info(f"Ejecutando query {query}...")
    conn.execute(query)
    logging.info(f"Se ejecuto correctamente la query: {query}")

# Get parameters from OpenAQ API
endpoint = "parameters"
json_api = get_data(base_url, endpoint, headers)
df_api = pd.json_normalize(data=json_api, sep="_")

with engine.connect() as conn, conn.begin():
    conn.execute("TRUNCATE TABLE stg_parameters;")
    load_to_sql(
        df=df_api,
        table_name="stg_parameters",
        engine=engine,
        if_exists="append",
    )
    query = f"CALL {schema}.petl_load_dim_parameter();"
    logging.info(f"Ejecutando query {query}...")
    conn.execute(query)
    logging.info(f"Se ejecuto correctamente la query: {query}")

# Get measures from OpenAQ API
endpoint = "measurements"
start_date = datetime.datetime.utcnow() - datetime.timedelta(
    hours=24
)  # datos de las ultimas 24hs..
params = {"date_from": start_date}

json_api = get_data(base_url, endpoint, headers, params=params)
df_api = pd.json_normalize(data=json_api, sep="_")

with engine.connect() as conn, conn.begin():
    conn.execute("TRUNCATE TABLE stg_measurements;")
    load_to_sql(
        df=df_api,
        table_name="stg_measurements",
        engine=engine,
        if_exists="append",
    )
    query = f"CALL {schema}.petl_load_fact_measure();"
    logging.info(f"Ejecutando query {query}...")
    conn.execute(query)
    logging.info(f"Se ejecuto correctamente la query: {query}")
