import pandas as pd


def formatear_fechas_ambulatorio(df_procesada):
    tmp = df_procesada.copy()

    tmp["Fecha Nacimiento"] = pd.to_datetime(
        tmp["Fecha Nacimiento"], errors="coerce", dayfirst=True
    )
    tmp["Fecha Reserva"] = pd.to_datetime(tmp["Fecha Reserva"], yearfirst=True)
    tmp["Fecha Atención"] = pd.to_datetime(tmp["Fecha Atención"], yearfirst=True)

    return tmp
