import time

import pandas as pd


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


def decorador_tiempo(func):
    def wrapper(*args, **kwargs):
        inicio = time.time()
        resultado = func(*args, **kwargs)
        fin = time.time()
        tiempo_ejecucion = fin - inicio
        print(f">> {func.__name__} se ejecutó en {tiempo_ejecucion:.4f} segundos")
        return resultado

    return wrapper


def unificar_formato_ruts(columna_ruts: pd.Series, eliminar_digito_verificador=True) -> pd.Series:
    """
    Funcion que consolida el formato de los RUTs de una persona. Elimina puntos, guiones y deja
    sin digito verificador. Los RUTs entrantes DEBEN tener el digito verificador si o si.

    Parámetros:
    columna_ruts (pd.Series): Serie de pandas que contiene los RUTs a anonimizar.

    Retorna:
    pd.Series: Serie de pandas con los RUTs anonimizados.
    """
    # Elimina puntos, guiones, espacios y 0s al inicio. Ademas convierte las letras a mayuscula
    ruts_limpios = (
        columna_ruts.astype(str)
        .str.replace(r"\.|-|\s", "", regex=True)
        .str.strip()
        .str.upper()
        .str.lstrip("0")
    )

    if eliminar_digito_verificador:
        # Elimina el digito verificador si es que lo tiene
        ruts_limpios = ruts_limpios.str[:-1]

    # Elimina las K que podrian haber quedado al final luego de la limpiza (por haber imputado -Kk)
    ruts_limpios = ruts_limpios.str.rstrip("K")

    return ruts_limpios


def limpiar_columna_texto(serie):
    return (
        serie.str.upper()
        .str.strip()
        .str.normalize("NFD")
        .str.encode("ascii", "ignore")
        .str.decode("utf-8")
    )
