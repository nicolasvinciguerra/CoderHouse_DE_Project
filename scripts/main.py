from configparser import ConfigParser
import pandas as pd
from datetime import datetime, timedelta
from utils import *
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

base_url = "https://api.openaq.org/v2"


def load_locations(config_file, limit=1000):
    """
    Get locations from OpenAQ API
    """

    # Config file
    config = ConfigParser()
    config.read(config_file)
    logging.info("Archivo de configuracion leido correctamente.")

    # API config variables
    api_key = config["openaq_api_credentials"]["api_key"]
    headers = {
        "X-API-Key": api_key,
        "accept": "application/json",
        "content-type": "application/json",
    }
    endpoint = "locations"
    params = {"limit": limit}

    json_api = get_data(base_url, endpoint, headers, params=params)
    df_api = pd.json_normalize(data=json_api, sep="_")
    df_api.drop(
        ["parameters", "measurements", "manufacturers", "bounds"], axis=1, inplace=True
    )

    engine = connect_to_db(config, "redshift_credentials")
    schema = config["redshift_credentials"]["schema"]
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


def load_countries(config_file, limit=1000):
    """
    Get countries from OpenAQ API
    """

    # Config file
    config = ConfigParser()
    config.read(config_file)
    logging.info("Archivo de configuracion leido correctamente.")

    # API config variables
    api_key = config["openaq_api_credentials"]["api_key"]
    headers = {
        "X-API-Key": api_key,
        "accept": "application/json",
        "content-type": "application/json",
    }
    endpoint = "countries"
    params = {"limit": limit}

    json_api = get_data(base_url, endpoint, headers, params=params)
    df_api = pd.json_normalize(data=json_api, sep="_")

    engine = connect_to_db(config, "redshift_credentials")
    schema = config["redshift_credentials"]["schema"]
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


def load_parameters(config_file):
    """
    Get parameters from OpenAQ API
    """
    # Config file
    config = ConfigParser()
    config.read(config_file)
    logging.info("Archivo de configuracion leido correctamente.")

    # API config variables
    api_key = config["openaq_api_credentials"]["api_key"]
    headers = {
        "X-API-Key": api_key,
        "accept": "application/json",
        "content-type": "application/json",
    }
    endpoint = "parameters"

    json_api = get_data(base_url, endpoint, headers)
    df_api = pd.json_normalize(data=json_api, sep="_")

    engine = connect_to_db(config, "redshift_credentials")
    schema = config["redshift_credentials"]["schema"]
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


def load_measures(config_file, start_date, end_date):
    """
    Get measures from OpenAQ API
    """

    # Config file
    config = ConfigParser()
    config.read(config_file)
    logging.info("Archivo de configuracion leido correctamente.")

    # API config variables
    api_key = config["openaq_api_credentials"]["api_key"]
    headers = {
        "X-API-Key": api_key,
        "accept": "application/json",
        "content-type": "application/json",
    }
    endpoint = "measurements"
    params = {"date_from": start_date, "date_to": end_date}

    json_api = get_data(base_url, endpoint, headers, params=params)
    df_api = pd.json_normalize(data=json_api, sep="_")

    engine = connect_to_db(config, "redshift_credentials")
    schema = config["redshift_credentials"]["schema"]
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


if __name__ == "__main__":
    load_locations("config/config.ini")
    load_countries("config/config.ini")
    load_parameters("config/config.ini")

    start = datetime.now() - timedelta(hours=1)
    end = start.strftime("%Y-%m-%d %H:59:59")
    start = start.strftime("%Y-%m-%d %H:00:00")
    load_measures("config/config.ini", start, end)
