from ast import List
from atexit import _run_exitfuncs
import logging
from re import L
import requests
from configparser import ConfigParser
import sqlalchemy as sa
from sqlalchemy.engine.url import URL
import smtplib
from email.message import EmailMessage

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_data(base_url, endpoint, headers, params=None):
    """
    Realiza una solicitud GET a una API para obtener datos

    :param base_url: la URL base de la API.
    :param endpoint: El endpoint de la API para obtener datos especificos.
    :param headers: Los headers para la solicitud GET.
    :param params: Parametros de la solicitud GET.

    Returns:
    JSON: Un archivo plano JSON con los datos obtenidos de la API.
    """
    try:
        logging.info(f"Obteniendo datos de {base_url}/{endpoint}...")
        response = requests.get(
            f"{base_url}/{endpoint}", headers=headers, params=params
        )
        response.raise_for_status()
        try:
            data = response.json()["results"]
        except:
            logging.error(
                "Error en la extraccion de resultados del request. Formato de respuesta no esperado."
            )
            raise Exception(f"La peticion ha fallado. Codigo del error: {e}")
        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"La peticion ha fallado. Codigo del error: {e}")
        raise Exception(f"La peticion ha fallado. Codigo del error: {e}")


def connect_to_db(config: ConfigParser, config_section):
    """
    Crea conexion a la base de datos
    a partir del archivo de configuracion.
    :param config(ConfigParser): instancia de un objeto tipo configparser
    :param config_section(str): seccion dentro del archivo de configuracion para la coneccion

    Returns:
    sqlalchemy.engine.base.Engine: Un objeto de conexión a la base de datos.
    """

    try:
        if not config.has_section(config_section):
            raise Exception(
                f"No se encontró la sección {config_section} en el archivo de configuracion."
            )

        # construye sqlalchemy URL
        url = URL.create(
            drivername=config[config_section][
                "driver"
            ],  # indicate driver and dialect will be used
            host=config[config_section]["host"],
            port=config[config_section]["port"],
            database=config[config_section]["database"],
            username=config[config_section]["username"],
            password=config[config_section]["password"],
        )

        engine = sa.create_engine(url)
        logging.info("Conexión a la base de datos establecida exitosamente.")
        return engine
    except Exception as e:
        logging.error(f"Error al conectarse a la base de datos: {e}.")
        raise Exception(f"Error al conectarse a la base de datos: {e}.")


def load_to_sql(df, table_name, engine, if_exists="replace"):
    """
    Carga un DataFrame en la base de datos especificada.

    Parameters:
    df (pandas.DataFrame): El DataFrame a cargar en la base de datos.
    table_name (str): El nombre de la tabla en la base de datos.
    engine (sqlalchemy.engine.base.Engine): Un objeto de conexión a la base de datos.
    """
    try:
        logging.info(f"Cargando datos en la tabla {table_name}...")
        df.to_sql(
            table_name,
            engine,
            if_exists=if_exists,
            method="multi",
            index=False,
        )
        logging.info(f"Datos cargados exitosamente en la tabla {table_name}.")
    except Exception as e:
        logging.error(f"Error al cargar los datos en la base de datos: {e}")
        raise Exception(f"Error al cargar los datos en la base de datos: {e}")


def send_gmail_email(
    sender_email, private_key, subject, body_text, recipients_email: list
):
    try:
        email = EmailMessage()
        email["From"] = sender_email
        email["Subject"] = subject
        email.set_content(body_text, subtype="html")
        x = smtplib.SMTP("smtp.gmail.com", 587)
        x.starttls()
        x.login(sender_email, private_key)
        for recipient in recipients_email:
            email["To"] = recipient
            x.send_message(email)
            logging.info(f"Email enviado correctamente a {recipient}.")
        x.quit()
    except Exception as e:
        logging.error(f"Error al enviar email: {e}")
        raise Exception(f"Error al enviar email: {e}")
