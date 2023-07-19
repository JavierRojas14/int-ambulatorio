# -*- coding: utf-8 -*-
import glob
import hashlib
import json
import logging
import os

from datetime import datetime
from pathlib import Path

import click
import pandas as pd
from dotenv import find_dotenv, load_dotenv

COLS_A_OCUPAR = {
    "Código Reserva Atención": str,
    "Rut Paciente": str,
    "Nombre Paciente": str,
    "Apellido Paterno Paciente": str,
    "Apellido Materno Paciente": str,
    "Fecha Nacimiento": datetime,
    "sexo": str,
    "Fecha Reserva": datetime,
    "Fecha Atención": datetime,
    "Rut Profesional": str,
    "Nombre Especialidad": str,
    "Código Diagnóstico": str,
    "Nombre Diagnóstico": str,
    "Detalle Atención": str,
    "Año": pd.Int16Dtype,
}

COLS_A_HASHEAR = [
    "Rut Paciente",
    "Rut Profesional",
]

COLS_A_ELIMINAR = [
    "Nombre Paciente",
    "Apellido Paterno Paciente",
    "Apellido Materno Paciente",
]


def salted_sha256_anonymize(df, columns_to_anonymize):
    anonymized_df = df.copy()

    salts = {}

    for column in columns_to_anonymize:
        # Generate a random salt for each column
        salt = os.urandom(16)
        salts[column] = salt.hex()

        # Pseudonymize the column values with salted SHA-256
        anonymized_df[column] = df.apply(
            lambda row: hashlib.sha256(salt + str(row[column]).encode()).hexdigest(), axis=1
        )

    # Save the salts to a JSON file
    with open("data/processed/salts.json", "w") as file:
        json.dump(salts, file, indent=1)

    return anonymized_df


def preprocesar_diagnostico(serie_diagnostico):
    return serie_diagnostico.str.replace("\.|\s|_", "", regex=True)


def preprocesar_sexo(serie_sexo):
    return serie_sexo.str.strip().str.lower()


def unir_filas_repetidas(df, columnas_repetidas, columna_distinta):
    tmp = df.groupby(columnas_repetidas)[columna_distinta].apply(", ".join).reset_index()
    return tmp


def leer_y_preprocesar_ambulatorio_diagnosticos(input_filepath):
    df = pd.concat(
        (
            pd.read_excel(archivo, usecols=COLS_A_OCUPAR.keys())
            for archivo in glob.glob(f"{input_filepath}/diagnosticos/*.xlsx")
        )
    )

    df = df.drop(columns=COLS_A_ELIMINAR)

    df["Código Diagnóstico"] = preprocesar_diagnostico(df["Código Diagnóstico"].astype(str))
    df["sexo"] = preprocesar_sexo(df["sexo"])

    columnas_repetidas = [
        "Código Reserva Atención",
        "Rut Paciente",
        "Fecha Nacimiento",
        "sexo",
        "Fecha Reserva",
        "Fecha Atención",
        "Rut Profesional",
        "Nombre Especialidad",
        "Código Diagnóstico",
        "Nombre Diagnóstico",
        "Año",
    ]
    columna_no_repetida = "Detalle Atención"
    df[columna_no_repetida] = df[columna_no_repetida].astype(str)

    df = unir_filas_repetidas(df, columnas_repetidas, columna_no_repetida)
    df = salted_sha256_anonymize(df, COLS_A_HASHEAR)
    df = df.rename(columns={"Rut Paciente": "ID_PACIENTE", "Rut Profesional": "ID_PROFESIONAL"})
    df = clean_column_names(df)

    return df


########################## Procedimientos ####################################


def clean_column_names(df):
    tmp = df.copy()

    # Clean and transform the column names using vectorization
    cleaned_columns = (
        df.columns.str.lower()
        .str.normalize("NFD")
        .str.encode("ascii", "ignore")
        .str.decode("utf-8")
    )
    cleaned_columns = cleaned_columns.str.replace(" ", "_")

    # Assign the cleaned column names back to the DataFrame
    tmp.columns = cleaned_columns

    return tmp


def leer_y_preprocesar_ambulatorio_procedimientos(input_filepath):
    df = pd.concat(
        (
            pd.read_excel(archivo)
            for archivo in glob.glob(f"{input_filepath}/procedimientos/*.xlsx")
        )
    ).drop(columns=["N°", "Nombre", "Médico"])


    df = df.rename(columns={"Columna1": "unidad_que_la_realiza"})
    df = clean_column_names(df)
    df = df.query("`cerrado/abierto` == 'ABIERTA'")
    df["rut"] = df.rut.str.lower().str.replace("\.|-|\s", "", regex=True)
    df["rut_cortado"] = df.rut.str[:-1]

    with open("data/processed/salts.json", encoding="utf-8") as file:
        sales = json.load(file)
        sal_rut = sales["Rut Paciente"]

    df["id_paciente"] = (
        bytes.fromhex(sal_rut) + df["rut_cortado"].str.encode(encoding="utf-8")
    ).apply(lambda x: hashlib.sha256(x).hexdigest())

    df = df.drop(columns=["rut", "rut_cortado"])

    cols_strings_sin_espacios = df.select_dtypes(include="object").apply(lambda x: x.str.strip())
    df.loc[:, cols_strings_sin_espacios.columns] = cols_strings_sin_espacios

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
    df_procedimientos = leer_y_preprocesar_ambulatorio_procedimientos(input_filepath)

    df_diagnosticos.to_csv(
        f"{output_filepath}/datos_limpios_diagnosticos.csv",
        encoding="latin-1",
        index=False,
        sep=";",
        errors="replace",
    )
    df_procedimientos.to_csv(
        f"{output_filepath}/datos_limpios_procedimientos.csv",
        encoding="latin-1",
        index=False,
        sep=";",
        errors="replace",
    )


if __name__ == "__main__":
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
