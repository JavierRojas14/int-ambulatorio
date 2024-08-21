import pandas as pd
import glob

from make_dataset import clean_column_names


def leer_procedimientos(input_filepath):
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

    df = df.drop(columns=["rut", "rut_cortado"])

    return df
