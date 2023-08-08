import pandas as pd
import numpy as np


def formatear_fechas_ambulatorio(df_procesada):
    tmp = df_procesada.copy()

    tmp["fecha_nacimiento"] = pd.to_datetime(
        tmp["fecha_nacimiento"], errors="coerce", dayfirst=True
    )
    tmp["fecha_reserva"] = pd.to_datetime(tmp["fecha_reserva"], yearfirst=True)
    tmp["fecha_atencion"] = pd.to_datetime(tmp["fecha_atencion"], yearfirst=True)

    return tmp


def formatear_fechas_procedimientos(df_procesada):
    tmp = df_procesada.copy()
    tmp["fecha_realizacion"] = pd.to_datetime(
        tmp.fecha_realizacion, yearfirst=True, errors="coerce"
    )

    return tmp


def agregar_rango_etario(df_procesada):
    tmp = df_procesada.copy()

    edad_primera_consulta = (tmp["fecha_atencion"] - tmp["fecha_nacimiento"]) / np.timedelta64(
        1, "Y"
    )

    tmp["edad_primera_consulta"] = edad_primera_consulta
    maxima_edad = int(round(max(edad_primera_consulta), 0))

    tmp["rango_etario_primera_consulta"] = pd.cut(
        edad_primera_consulta, range(0, maxima_edad + 1, 10)
    )

    return tmp


def agregar_anio_mes_dia(df, datetime_column):
    """
    Adds the year, month, and day columns to the dataframe.

    Args:
      df: The dataframe to add the columns to.
      datetime_column: The name of the datetime column.

    Returns:
      The dataframe with the year, month, and day columns appended.
    """

    df["year"] = df[datetime_column].dt.year
    df["month"] = df[datetime_column].dt.month
    df["day"] = df[datetime_column].dt.day

    return df


def conteo_agrupado_de_variable(
    df, vars_groupby, col_a_contar, cols_para_llave, variable_analizada
):
    """
    Perform grouped counting and aggregation of a variable in a DataFrame.

    Args:
        df (pandas.DataFrame): The input DataFrame.
        vars_groupby (list[str]): Columns by which to group the DataFrame.
        col_a_contar (str): Column containing values to be counted.
        cols_para_llave (list[str]): Columns to be used as keys for aggregation.
        variable_analizada (str): Name of the variable being analyzed.

    Returns:
        pandas.DataFrame: A DataFrame containing grouped counts and aggregated data.
    """
    columnas_finales = vars_groupby + [col_a_contar]
    if not all(col in columnas_finales for col in cols_para_llave):
        raise ValueError("Tus columnas para hacer la llave estan ausentes en las columnas finales. "
                         "Debes utilizar columnas que esten en la agrupacion + variable de conteo "
                         "para hacer una llave valida")

    if df.empty:
        return None

    resultado = (
        df.groupby(vars_groupby, dropna=False)[col_a_contar]
        .value_counts()
        .reset_index(name=f"conteo_{variable_analizada}")
    )

    resultado["llave_id"] = resultado[cols_para_llave].astype(str).apply("-".join, axis=1)
    return resultado


def obtener_desglose_sociodemografico(
    df, vars_groupby_estatico, vars_groupby_dinamico, col_a_contar
):
    """
    Obtain demographic breakdown of a specified variable within static and dynamic groups.

    Args:
        df (pandas.DataFrame): The input DataFrame.
        vars_groupby_estatico (list[str]): Columns for static grouping.
        vars_groupby_dinamico (list[str]): Columns for dynamic grouping.
        col_a_contar (str): Column containing values to be counted.

    Returns:
        dict: A dictionary containing breakdown results for static and dynamic groups.
    """

    # Aqui se va a tener un groupby estatico. Ej: Anios
    # Se va a tener un groupby dinamico. Ej: Sexo, Otros
    # Y se va a tener una variable que se va a contar. Ej: Diagnostico

    dict_resultados = {}
    cols_para_llave = vars_groupby_estatico + [col_a_contar]

    conteo_global = conteo_agrupado_de_variable(
        df, vars_groupby_estatico, col_a_contar, cols_para_llave, "global"
    )

    dict_resultados["global"] = conteo_global

    for variable in vars_groupby_dinamico:
        # Por lo tanto, aqui el groupby que se hara sera ["Anios", "Sexo"].
        desglose_dinamico = vars_groupby_estatico + [variable]
        conteo_dinamico = conteo_agrupado_de_variable(
            df, desglose_dinamico, col_a_contar, cols_para_llave, variable
        )
        conteo_dinamico = conteo_dinamico.drop(columns=cols_para_llave)

        dict_resultados[variable] = conteo_dinamico

    return dict_resultados


def obtener_diag_mas_cercano(df_paciente, fecha_procedimiento):
    diferencias_fecha = abs(fecha_procedimiento - df_paciente["fecha_atencion"])
    indice_fecha_mas_cercana = diferencias_fecha[diferencias_fecha == min(diferencias_fecha)].index[
        0
    ]
    diag_mas_cercano = df_paciente.loc[indice_fecha_mas_cercana, "codigo_diagnostico"]

    return diag_mas_cercano
