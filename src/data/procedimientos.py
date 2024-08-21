"""Funciones para procesar la base de datos de procedimientos del INT
"""

import glob

import pandas as pd
from funciones_auxiliares import (
    clean_column_names,
    decorador_tiempo,
    unificar_formato_ruts,
)


@decorador_tiempo
def leer_procedimientos(input_filepath):
    """
    Reads and preprocesses procedure data from multiple Excel files.

    :param input_filepath: The path to the input directory.
    :type input_filepath: str

    :return: The preprocessed procedure DataFrame.
    :rtype: pandas DataFrame
    """
    # Lee las bases de procedimientos
    ruta_archivos = f"{input_filepath}/procedimientos/*.xlsx"
    df = pd.concat((pd.read_excel(archivo) for archivo in glob.glob(ruta_archivos)))

    # Elimina las columnas innecesarias
    df = df.drop(columns=["N°", "Nombre", "Médico"])

    # Renombra columna 1
    df = df.rename(columns={"Columna1": "unidad_que_la_realiza"})

    # Limpia los nombres de las columnas
    df = clean_column_names(df)

    # Solamente deja los procedimientos de atencion abierta
    df = df.query("cerradoabierto == 'ABIERTA'").copy()

    # Limpia los RUTs
    df["id_paciente"] = unificar_formato_ruts(df["rut"], eliminar_digito_verificador=True)

    # Elimina las columnas de RUTs
    df = df.drop(columns=["rut", "rut_cortado"])

    return df
