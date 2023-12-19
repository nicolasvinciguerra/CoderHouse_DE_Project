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


def alert_avg_values(config_file, exec_date, sender_email, private_key):
    """
    Send alerting emails with countries whose setting parameters are above the average.
    """

    # Config file
    config = ConfigParser()
    config.read(config_file)
    logging.info("Archivo de configuracion leido correctamente.")

    # config variables
    parameters = config["alerting"]["parameters"]
    parameters_list = [p.strip() for p in parameters.split(",")]
    email_recipients = config["alerting"]["email_recipients"]
    email_recipients_list = [e.strip() for e in email_recipients.split(",")]

    engine = connect_to_db(config, "redshift_credentials")
    schema = config["redshift_credentials"]["schema"]
    with engine.connect() as conn, conn.begin():
        for parameter in parameters_list:
            query = f"""
                    select fm.country_code, date(fm.date_utc), fm.parameter_code, avg(fm.value)
                    from {schema}.fact_measure fm
                    where date(fm.date_utc) = '{exec_date}'  and fm.parameter_code='{parameter}'
                    group by fm.country_code, date(fm.date_utc), fm.parameter_code
                    having avg(fm.value) > (select avg(value)
						                    from {schema}.fact_measure
						                    where date(date_utc) = '{exec_date}'  and parameter_code='{parameter}')
                    """
            data = pd.read_sql_query(query, conn)
            logging.info(
                f"Datos leidos correctamente para {parameter} con fecha {exec_date}. Dataframe: {data}."
            )
            if not data.empty:
                email_subject = f"[ALERT] Abnormal values on {parameter}"
                email_body = f"""\
                            <html><head></head><body>
                            <p>Countries with {parameter} value above the daily average for ({exec_date}):</p>
                            {data.to_html(index=False)}
                            </body></html>"""
                send_gmail_email(
                    sender_email,
                    private_key,
                    email_subject,
                    email_body,
                    email_recipients_list,
                )
                logging.info(
                    f"Emails de alerta para {parameter} con fecha {exec_date} enviado."
                )
            else:
                logging.info(
                    f"No se han enviado emails de alerta para {parameter} con fecha {exec_date} por no haber datos que cumplan la regla."
                )


if __name__ == "__main__":
    load_locations("config/config.ini")
    load_countries("config/config.ini")
    load_parameters("config/config.ini")

    start = datetime.now() - timedelta(hours=1)
    end = start.strftime("%Y-%m-%d %H:59:59")
    start = start.strftime("%Y-%m-%d %H:00:00")
    load_measures("config/config.ini", start, end)
