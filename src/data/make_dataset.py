# -*- coding: utf-8 -*-
import logging
import time

from pathlib import Path
from functools import wraps

import click
from dotenv import find_dotenv, load_dotenv


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
