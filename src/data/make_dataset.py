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


def anonymize_value(row, column, salt):
    """
    Anonymizes a single value using salted SHA-256 hashing.

    :param row: The row of the DataFrame.
    :type row: pandas Series
    :param column: The column name.
    :type column: str
    :param salt: The salt value.
    :type salt: bytes

    :return: The anonymized value.
    :rtype: str
    """
    return hashlib.sha256(salt + str(row[column]).encode()).hexdigest()


def salted_sha256_anonymize(df, columns_to_anonymize):
    """
    Anonymizes specified columns in a DataFrame using salted SHA-256 hashing.

    :param df: The input DataFrame.
    :type df: pandas DataFrame
    :param columns_to_anonymize: A list of column names to be anonymized.
    :type columns_to_anonymize: list

    :return: The anonymized DataFrame.
    :rtype: pandas DataFrame
    """
    anonymized_df = df.copy()

    salts = {}

    for column in columns_to_anonymize:
        # Generate a random salt for each column
        salt = os.urandom(16)
        salts[column] = salt.hex()

        # Pseudonymize the column values with salted SHA-256
        anonymized_df[column] = df.apply(anonymize_value, args=(column, salt), axis=1)

    # Save the salts to a JSON file
    with open("data/processed/salts.json", "w") as file:
        json.dump(salts, file, indent=1)

    return anonymized_df


def preprocesar_diagnostico(serie_diagnostico):
    """
    Preprocesses a series of diagnoses by removing special characters.

    :param serie_diagnostico: The series of diagnoses.
    :type serie_diagnostico: pandas Series

    :return: The preprocessed series of diagnoses.
    :rtype: pandas Series
    """
    return serie_diagnostico.str.replace("\.|\s|_", "", regex=True)


def preprocesar_sexo(serie_sexo):
    """
    Preprocesses a series of genders by removing leading/trailing whitespaces and
    converting to lowercase.

    :param serie_sexo: The series of genders.
    :type serie_sexo: pandas Series

    :return: The preprocessed series of genders.
    :rtype: pandas Series
    """
    return serie_sexo.str.strip().str.lower()


def unir_filas_repetidas(df, columnas_repetidas, columna_distinta):
    """
    Concatenates rows with duplicate values in specified columns, joining the distinct
    values in another column.

    :param df: The input DataFrame.
    :type df: pandas DataFrame
    :param columnas_repetidas: A list of column names with duplicate values.
    :type columnas_repetidas: list
    :param columna_distinta: The column containing distinct values to be joined.
    :type columna_distinta: str

    :return: The DataFrame with concatenated rows.
    :rtype: pandas DataFrame
    """
    if df.empty:
        return pd.DataFrame()

    tmp = (
        df.groupby(columnas_repetidas, dropna=False)[columna_distinta]
        .apply(", ".join)
        .reset_index()
    )
    return tmp


def obtener_diccionario_traductor_diags():
    archivo = pd.ExcelFile("data/external/diagnosticos_encontrados_asignados_cie_10.xlsx")
    df = pd.concat((pd.read_excel(archivo, sheet_name=hoja) for hoja in archivo.sheet_names))

    diagnosticos_anomalos_con_cie = df[~df["CIE 10"].isna()][["codigo_diagnostico", "CIE 10"]]
    diagnosticos_anomalos_con_cie["codigo_diagnostico"] = diagnosticos_anomalos_con_cie[
        "codigo_diagnostico"
    ].astype(str)
    diccionario_diags = diagnosticos_anomalos_con_cie.set_index("codigo_diagnostico").to_dict()[
        "CIE 10"
    ]

    return diccionario_diags


def modificar_diags_largo_3(df):
    tmp = df.copy()

    mask_largo_3 = tmp["codigo_diagnostico"].str.len() == 3
    tmp.loc[mask_largo_3, "codigo_diagnostico"] = tmp.loc[
        mask_largo_3, "codigo_diagnostico"
    ].str.ljust(4, "X")

    return tmp


def modificar_diags_largo_5(df):
    tmp = df.copy()

    mask_largo_5_con_guion = (tmp["codigo_diagnostico"].str.len() == 5) & ~(
        tmp["codigo_diagnostico"].str.contains("-")
    )

    tmp.loc[mask_largo_5_con_guion, "codigo_diagnostico"] = tmp.loc[
        mask_largo_5_con_guion, "codigo_diagnostico"
    ].str[:4]

    return tmp


def leer_y_preprocesar_ambulatorio_diagnosticos(input_filepath):
    """
    Reads and preprocesses diagnostic data from multiple Excel files.

    :param input_filepath: The path to the input directory.
    :type input_filepath: str

    :return: The preprocessed diagnostic DataFrame.
    :rtype: pandas DataFrame
    """
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
    # df = salted_sha256_anonymize(df, COLS_A_HASHEAR)
    df = df.rename(columns={"Rut Paciente": "ID_PACIENTE", "Rut Profesional": "ID_PROFESIONAL"})
    df = clean_column_names(df)

    diccionario_diagnosticos = obtener_diccionario_traductor_diags()
    df["codigo_diagnostico"] = df["codigo_diagnostico"].replace(diccionario_diagnosticos)
    df = modificar_diags_largo_3(df)
    df = modificar_diags_largo_5(df)

    return df


########################## Procedimientos ####################################


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
