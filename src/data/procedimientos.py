import pandas as pd
import glob

from funciones_auxiliares import clean_column_names


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
    df = df.query("`cerrado/abierto` == 'ABIERTA'").copy()

    # Limpia los RUTs
    df["rut"] = df.rut.str.lower().str.replace("\.|-|\s", "", regex=True)
    df["rut_cortado"] = df.rut.str[:-1]

    # Elimina las columnas de RUTs
    df = df.drop(columns=["rut", "rut_cortado"])

    return df
