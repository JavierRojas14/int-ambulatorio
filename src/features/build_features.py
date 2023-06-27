import pandas as pd
import numpy as np


def formatear_fechas_ambulatorio(df_procesada):
    tmp = df_procesada.copy()

    tmp["Fecha Nacimiento"] = pd.to_datetime(
        tmp["Fecha Nacimiento"], errors="coerce", dayfirst=True
    )
    tmp["Fecha Reserva"] = pd.to_datetime(tmp["Fecha Reserva"], yearfirst=True)
    tmp["Fecha Atención"] = pd.to_datetime(tmp["Fecha Atención"], yearfirst=True)

    return tmp


def agregar_rango_etario(df_procesada):
    tmp = df_procesada.copy()

    edad_primera_consulta = (tmp["Fecha Atención"] - tmp["Fecha Nacimiento"]) / np.timedelta64(
        1, "Y"
    )

    tmp["EDAD_PRIMERA_CONSULTA"] = edad_primera_consulta
    maxima_edad = int(round(max(edad_primera_consulta), 0))

    tmp["RANGO_ETARIO_PRIMERA_CONSULTA"] = pd.cut(
        edad_primera_consulta, range(0, maxima_edad + 1, 10)
    )

    return tmp


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

        dict_resultado[variable] = resultado

    return dict_resultado
