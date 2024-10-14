"""Funciones para procesar la base de datos de procedimientos del INT
"""

import glob

import pandas as pd
from funciones_auxiliares import (
    clean_column_names,
    decorador_tiempo,
    limpiar_columna_texto,
    unificar_formato_ruts,
)


def leer_formato_antiguo_datos_estadisticos(input_filepath):
    print("> Leyendo Procedimientos en Formato Antiguo")
    # Lee la base de procedimiento antiguos
    ruta_archivos = f"{input_filepath}/procedimientos/formato_antiguo/*.xlsx"
    df = pd.concat((pd.read_excel(archivo) for archivo in glob.glob(ruta_archivos)))

    # Reordena las columnas de la base de datos
    df = df[
        [
            "Columna1",
            "Rut",
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
    orden_columnas_nuevas = [
        "Servicio Clinico",
        "Rut Paciente",
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
    df.columns = orden_columnas_nuevas

    # Agrega la cantidad de numero de examenes
    df = df[orden_columnas_nuevas].value_counts().reset_index(name="Número de veces")

    # Agrega un indicador del procedimientos
    df = df.reset_index(names="Evento").reset_index(names="N°")

    # Reordena por ultima vez las columnas
    df = df[
        [
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
            "Número de veces",
        ]
    ]

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

    # Formatea la fecha de realizacion del procedimiento y agrega columna de anio
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    df["ano"] = df["fecha"].dt.year
    df = df.sort_values("fecha")

    # Limpia las columnas de texto
    columnas_de_texto_a_limpiar = [
        "servicio_clinico",
        "sexo",
        "previson",
        "comuna_residencia",
        "accion_clinica",
        "tipo_atencion",
        "especialidad",
    ]
    df[columnas_de_texto_a_limpiar] = df[columnas_de_texto_a_limpiar].apply(limpiar_columna_texto)

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
