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


def add_year_month_day(df, datetime_column):
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


def obtener_dfs_para_desglose_sociodemografico(
    df, vars_groupby_estatico, vars_groupby_dinamico, col_diagnostico
):
    dict_resultado = {}
    cols_para_llave = vars_groupby_estatico + [col_diagnostico]
    for variable in vars_groupby_dinamico:
        nuevo_desglose = vars_groupby_estatico + [variable]
        if variable == col_diagnostico:
            nuevo_desglose.pop()

        resultado = (
            df.groupby(nuevo_desglose, dropna=True)[col_diagnostico]
            .value_counts()
            .reset_index(name="conteo")
        )

        resultado["llave_id"] = (
            resultado[cols_para_llave].astype(str).apply(lambda x: "-".join(x), axis=1)
        )

        if variable != col_diagnostico:
            resultado = resultado.drop(columns=cols_para_llave)

        dict_resultado[variable] = resultado

    return dict_resultado

def obtener_diag_mas_cercano(df_paciente, fecha_procedimiento):
    diferencias_fecha = abs(fecha_procedimiento - df_paciente["fecha_atencion"])
    indice_fecha_mas_cercana = diferencias_fecha[diferencias_fecha == min(diferencias_fecha)].index[
        0
    ]
    diag_mas_cercano = df_paciente.loc[indice_fecha_mas_cercana, "codigo_diagnostico"]

    return diag_mas_cercano
