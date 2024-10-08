# -*- coding: utf-8 -*-
import logging
from pathlib import Path

import click
from dotenv import find_dotenv, load_dotenv
from his import leer_his
from procedimientos import leer_procedimientos
from trackcare import leer_trackcare


@click.command()
@click.argument("input_filepath", type=click.Path(exists=True))
@click.argument("output_filepath", type=click.Path())
def main(input_filepath, output_filepath):
    """Runs data processing scripts to turn raw data from (../raw) into
    cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info("making final data set from raw data")

    # Lee bases de ambulatorio
    df_his = leer_his(input_filepath)
    df_procedimientos = leer_procedimientos(input_filepath)
    df_track = leer_trackcare(input_filepath)

    # Guarda bases de ambulatorio
    df_his.to_csv(f"{output_filepath}/his_procesada.csv", index=False)
    df_procedimientos.to_csv(f"{output_filepath}/procedimientos_procesada.csv", index=False)
    df_track.to_csv(f"{output_filepath}/trackcare_procesada.csv", index=False)


if __name__ == "__main__":
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
