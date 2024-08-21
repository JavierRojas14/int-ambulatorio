# -*- coding: utf-8 -*-
import logging
import time

from pathlib import Path
from functools import wraps

import click
from dotenv import find_dotenv, load_dotenv

from his import leer_his


@click.command()
@click.argument("input_filepath", type=click.Path(exists=True))
@click.argument("output_filepath", type=click.Path())
def main(input_filepath, output_filepath):
    """Runs data processing scripts to turn raw data from (../raw) into
    cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info("making final data set from raw data")

    df_diagnosticos = leer_his(input_filepath)
    # df_procedimientos = leer_y_preprocesar_ambulatorio_procedimientos(input_filepath)
    # df_track = leer_y_preprocesar_ambulatorio_trackcare(input_filepath)

    df_diagnosticos.to_csv(f"{output_filepath}/his_procesada.csv")
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
