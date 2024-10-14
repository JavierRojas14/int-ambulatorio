"""Funciones para procesar la base de datos de procedimientos del INT
"""

import glob

import pandas as pd
from funciones_auxiliares import (
    clean_column_names,
    decorador_tiempo,
    unificar_formato_ruts,
)


def leer_formato_antiguo_datos_estadisticos(input_filepath):
    print("> Leyendo Procedimientos en Formato Antiguo")
    # Lee la base de procedimiento antiguos
    ruta_archivos = f"{input_filepath}/procedimientos/formato_antiguo/*.xlsx"
    print(ruta_archivos)
    df = pd.concat((pd.read_excel(archivo) for archivo in glob.glob(ruta_archivos)))

    # Reordena las columnas de la base de datos
    df = df[
        [
            "N°",
            "Columna1",
            "Rut",
            "N°",
            "Nombre",
            "Sexo",
            "Edad",
            "Previsión",
            "Comuna Residencia",
            "Fecha Realización",
            "Médico",
            "COD",
            "Glosa",
            "Cerrado/Abierto",
            "Subtipo",
        ]
    ]

    # Renombra las columnas para que esten en el nuevo formato
    df.columns = [
        "N°",
        "Servicio Clinico",
        "Rut Paciente",
        "Evento",
        "Nombre",
        "Sexo",
        "Edad",
        "Previsón",
        "Comuna Residencia",
        "Fecha",
        "Nombre Médico",
        "Codigo Accion Clinica",
        "Accion Clinica",
        "Tipo Atención",
        "Especialidad",
    ]

    # Agrega la cantidad de numero de examenes
    df["Número de veces"] = 1

    return df


@decorador_tiempo
def leer_procedimientos(input_filepath):
    """
    Reads and preprocesses procedure data from multiple Excel files.

    :param input_filepath: The path to the input directory.
    :type input_filepath: str

    :return: The preprocessed procedure DataFrame.
    :rtype: pandas DataFrame
    """
    print("> Leyendo Procedimientos")
    # Lee las bases de procedimientos
    ruta_archivos = f"{input_filepath}/procedimientos/*.xlsx"
    df = pd.concat((pd.read_excel(archivo) for archivo in glob.glob(ruta_archivos)))

    # Elimina las columnas innecesarias
    df = df.drop(columns=["N°", "Nombre", "Nombre Médico"])

    # Limpia los nombres de las columnas
    df = clean_column_names(df)

    # Limpia los RUTs
    df["id_paciente"] = unificar_formato_ruts(df["rut_paciente"], eliminar_digito_verificador=True)

    # Elimina las columnas de RUTs
    df = df.drop(columns=["rut_paciente"])

    # Agrega la columna del anio
    df["ano"] = pd.to_datetime(df["fecha"], errors="coerce").dt.year

    return df


if __name__ == "__main__":
    # Lee la base de datos de DDEE antigua y la transforma
    df_antigua = leer_formato_antiguo_datos_estadisticos("data/raw")
    df_antigua.to_excel(
        "data/raw/procedimientos/datos_estadisticos_formato_nuevo_2020_2022.xlsx", index=False
    )

    # Lee el formato nuevo de datos estadisticos
    df = leer_procedimientos("data/raw")
    df.to_csv("data/processed/procedimientos_procesada.csv", index=False)
