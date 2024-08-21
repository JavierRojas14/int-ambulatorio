# -*- coding: utf-8 -*-
import glob
import hashlib
import json
import logging
import os
import time

from pathlib import Path
from functools import wraps

import click
import pandas as pd
from dotenv import find_dotenv, load_dotenv


COLUMNAS_UTILES_TRACKCARE = [
    "PAPMIID",
    "Sexo",
    "Edad",
    "Comuna",
    "Provincia",
    "Region",
    "Prevision",
    "Plan",
    "Prestacion",
    "TipoAtencion",
    "FechaCita",
    "HoraCita",
    "EstadoCita",
]

CAMBIO_COMUNA_TRACKCARE = {
    "Maipú": "MAIPU",
    "Chillán": "CHILLAN",
    "Chépica": "CHEPICA",
    "Concón": "VALPARAISO",
    "Copiapó": "COPIAPO",
    "Curicó": "CURICO",
    "Los Angeles": "LOS ÁNGELES",
    "Quilpué": "QUILPUE",
    "Valparaíso": "VALPARAISO",
    "María Elena": "MARIA ELENA",
    "Santa María": "SANTA MARIA",
    "Chillán Viejo": "CHILLAN VIEJO",
    "Tomé": "TOME",
}

GLOSAS_CONSULTAS_REPETIDAS = [
    "CR - Control Ges Epoc",
    "CR - Control Ges Asma",
    "CR-Atención presencial",
    "CR- Atencion Email",
    "Consulta Compleja Repetida",
    "C. Repetida Atención Telefónica",
    "C. Repetida Telemedicina",
]

GLOSAS_CONSULTAS_NUEVAS = [
    "CN - Post Operado",
    "CN - Consulta nueva GES",
    "CN - 15 días",
    "CN - Ingreso de Pacientes",
    "CN - Post Examen",
    "CN - Rehabilitacion Pulmonar",
    "CN - Post Operados",
    "CN-Atención presencial",
    "CN - Atencion Inmediata",
    "CN Telemedicina",
    "CN- Atencion Email",
    "CN - Control prioritario",
    "C. Nueva Atención Telefónica",
    "C. Nueva Telemedicina",
]

GLOSAS_CONSULTAS_PROCEDIMIENTOS = [
    "PROC - Toma de Muestra",
    "PROC - KTR Integral",
    "PROC - Test Ejercicios",
]

GLOSAS_CONSULTAS_MISCALENEAS = ["Consulta Abreviada", "Control Post Operado"]


def medir_tiempo(func):
    @wraps(func)
    def envoltura(*args, **kwargs):
        inicio = time.time()  # Marca el tiempo de inicio
        resultado = func(*args, **kwargs)  # Ejecuta la función original
        fin = time.time()  # Marca el tiempo de finalización
        tiempo_transcurrido = fin - inicio  # Calcula el tiempo transcurrido

        # Imprime el nombre de la función y el tiempo de ejecución
        print(f"Función '{func.__name__}' ejecutada en {tiempo_transcurrido:.4f} segundos")

        return resultado  # Devuelve el resultado original de la función

    return envoltura


def clean_column_names(df):
    """
    Cleans the column names of a DataFrame by converting to lowercase, replacing spaces with
    underscores, ensuring only a single underscore between words, and removing miscellaneous symbols.

    :param df: The input DataFrame.
    :type df: pandas DataFrame

    :return: The DataFrame with cleaned column names.
    :rtype: pandas DataFrame
    """
    tmp = df.copy()

    # Clean and transform the column names
    cleaned_columns = (
        df.columns.str.lower()
        .str.normalize("NFD")
        .str.encode("ascii", "ignore")
        .str.decode("utf-8")
        .str.replace(
            r"[^\w\s]", "", regex=True
        )  # Remove all non-alphanumeric characters except spaces
        .str.replace(r"\s+", "_", regex=True)  # Replace spaces with underscores
        .str.replace(r"_+", "_", regex=True)  # Ensure only a single underscore between words
        .str.strip("_")
    )

    # Assign the cleaned column names back to the DataFrame
    tmp.columns = cleaned_columns

    return tmp


########################## Procedimientos ####################################


def leer_y_preprocesar_ambulatorio_procedimientos(input_filepath):
    """
    Reads and preprocesses procedure data from multiple Excel files.

    :param input_filepath: The path to the input directory.
    :type input_filepath: str

    :return: The preprocessed procedure DataFrame.
    :rtype: pandas DataFrame
    """
    df = pd.concat(
        (pd.read_excel(archivo) for archivo in glob.glob(f"{input_filepath}/procedimientos/*.xlsx"))
    ).drop(columns=["N°", "Nombre", "Médico"])

    df = df.rename(columns={"Columna1": "unidad_que_la_realiza"})
    df = clean_column_names(df)
    df = df.query("`cerrado/abierto` == 'ABIERTA'").copy()
    df["rut"] = df.rut.str.lower().str.replace("\.|-|\s", "", regex=True)
    df["rut_cortado"] = df.rut.str[:-1]

    with open("data/processed/salts.json", encoding="utf-8") as file:
        sales = json.load(file)
        sal_rut = sales["Rut Paciente"]

    df["id_paciente"] = (
        bytes.fromhex(sal_rut) + df["rut_cortado"].str.encode(encoding="utf-8")
    ).apply(lambda x: hashlib.sha256(x).hexdigest())

    df = df.drop(columns=["rut", "rut_cortado"])

    return df


################################ Datos Pacientes ########################################


def leer_y_preprocesar_ambulatorio_trackcare(input_filepath):
    df = pd.concat(
        (
            pd.read_csv(archivo, encoding="latin-1", sep="\t", usecols=COLUMNAS_UTILES_TRACKCARE)
            for archivo in glob.glob(f"{input_filepath}/trackcare/*.xls")
        )
    )

    df = clean_column_names(df)
    df["hora_completa_cita"] = pd.to_datetime(df["fechacita"] + " " + df["horacita"], dayfirst=True)

    df["id_paciente"] = (
        df["papmiid"].str.lower().str.replace("\.|-|\s", "", regex=True).str[:-1].astype(str)
    )

    with open("data/processed/salts.json", encoding="utf-8") as file:
        sales = json.load(file)
        sal_rut = sales["Rut Paciente"]

    df["id_paciente"] = (
        bytes.fromhex(sal_rut) + df["id_paciente"].str.encode(encoding="utf-8")
    ).apply(lambda x: hashlib.sha256(x).hexdigest())

    df["rango_etario"] = pd.cut(df.edad, bins=range(0, 151, 10))
    df["comuna"] = df["comuna"].replace(CAMBIO_COMUNA_TRACKCARE)
    df["tipoatencion"] = (
        df["tipoatencion"]
        .replace(GLOSAS_CONSULTAS_NUEVAS, "Consulta Nueva")
        .replace(GLOSAS_CONSULTAS_REPETIDAS, "Consulta Repetida")
        .replace(GLOSAS_CONSULTAS_PROCEDIMIENTOS, "Procedimiento")
        .replace(GLOSAS_CONSULTAS_MISCALENEAS, "Miscaleneo")
    )

    df = df.drop(columns=["papmiid"])

    return df


@click.command()
@click.argument("input_filepath", type=click.Path(exists=True))
@click.argument("output_filepath", type=click.Path())
def main(input_filepath, output_filepath):
    """Runs data processing scripts to turn raw data from (../raw) into
    cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info("making final data set from raw data")

    df_diagnosticos = leer_y_preprocesar_ambulatorio_diagnosticos(input_filepath)
    # df_procedimientos = leer_y_preprocesar_ambulatorio_procedimientos(input_filepath)
    # df_track = leer_y_preprocesar_ambulatorio_trackcare(input_filepath)

    df_diagnosticos.to_csv(
        f"{output_filepath}/datos_limpios_diagnosticos.csv",
        encoding="latin-1",
        index=False,
        sep=";",
        errors="replace",
    )
    # df_procedimientos.to_csv(
    #     f"{output_filepath}/datos_limpios_procedimientos.csv",
    #     encoding="latin-1",
    #     index=False,
    #     sep=";",
    #     errors="replace",
    # )

    # df_track.to_csv(
    #     f"{output_filepath}/datos_limpios_track.csv",
    #     encoding="latin-1",
    #     index=False,
    #     sep=";",
    #     errors="replace",
    # )


if __name__ == "__main__":
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
