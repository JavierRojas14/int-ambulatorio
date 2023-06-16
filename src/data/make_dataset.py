# -*- coding: utf-8 -*-
import hashlib
import logging
import glob
from pathlib import Path

import pandas as pd
from datetime import datetime

import click
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
    "Nombre Paciente",
    "Apellido Paterno Paciente",
    "Apellido Materno Paciente",
    "Rut Profesional",
]


def hashear_columna_texto(serie_texto):
    serie_hasheada = serie_texto.copy()

    serie_hasheada = serie_hasheada.astype(str).apply(
        lambda x: hashlib.sha512(x.encode()).hexdigest()
    )

    return serie_hasheada


def preprocesar_diagnostico(serie_diagnostico):
    return serie_diagnostico.str.replace("\.|\s|_", "", regex=True)

def preprocesar_sexo(serie_sexo):
    return serie_sexo.str.strip().str.lower()

@click.command()
@click.argument("input_filepath", type=click.Path(exists=True))
@click.argument("output_filepath", type=click.Path())
def main(input_filepath, output_filepath):
    """Runs data processing scripts to turn raw data from (../raw) into
    cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info("making final data set from raw data")
    df = pd.concat(
        (
            pd.read_excel(archivo, usecols=COLS_A_OCUPAR.keys())
            for archivo in glob.glob(f"{input_filepath}/*.xlsx")
        )
    )

    df.loc[:, COLS_A_HASHEAR] = df.loc[:, COLS_A_HASHEAR].apply(hashear_columna_texto)
    df["Código Diagnóstico"] = preprocesar_diagnostico(df["Código Diagnóstico"].astype(str))
    df["sexo"] = preprocesar_sexo(df["sexo"])
    df.to_csv(output_filepath, encoding="latin-1", index=False, sep=";", errors="replace")


if __name__ == "__main__":
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
