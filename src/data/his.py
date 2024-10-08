import glob

import pandas as pd
from funciones_auxiliares import clean_column_names, decorador_tiempo

COLUMNAS_UTILES_HIS = [
    "Código Reserva Atención",
    "Rut Paciente",
    "Fecha Nacimiento",
    "sexo",
    "Fecha Reserva",
    "Fecha Atención",
    "Nombre Especialidad",
    "Código Diagnóstico",
    "Nombre Diagnóstico",
    "Detalle Atención",
    "Año",
]

AGRUPACIONES_ESPECIALIDAD = {
    "Broncopulmonar": [
        "AVNIA",
        "BRONCOPULMONAR ADULTO INT",
        "BRONQUIECTACIAS",
        "COMPIN",
        "DERRAME PLEURAL",
        "EPOC TIOTROPIO",
        "ENFERMEDADES PROFESIONALES",
        "EX. BRONCOSCOPIA",
        "FIBROSIS PULMONAR",
        "GES ASMA",
        "GES EPOC",
        "GES FIBROSIS QUISTICA",
        "HIPERTENSION PULMONAR",
        "INGRESO PROGRAMA DE OXIGENO",
        "OXIGENOTERAPIA",
        "PESQUISA CANCER PULMONAR",
        "PRIORITARIO BRONCOPULMONAR",
        "PULMON REUMATOLÓGICO",
        "TABACO GRUPAL",
        "TABACO INDIVIDUAL",
        "TBC",
    ],
    "Cardiocirugia": [
        "CARDIOCIRUGIA",
        "CARDIOPATIA CONGENITA",
        "TRASPLANTE CARDIACO",
        "PATOLOGÍA DE LA AORTA TORÁCICA-MARFAN",
    ],
    "Cirugia de Torax": [
        "CIRUGIA DE TORAX",
        "ONCOLOGIA",
        "TRASPLANTE PULMONAR",
    ],
    "Unidad del Sueno": [
        "UNIDAD DE SUEÑO",
        "UNIDAD DE SUEÑO-OTORRINOLARINGOLOGO",
    ],
    "Cardiologia": [
        "ARRITMIA",
        "CARDIOLOGIA",
        "EX. ECOCARDIO URGENCIA",
        "EX. ECOCARDIOGRAMA",
        "EX. HOLTER CONGENITOS INT",
        "EX. TEST DE ESFUERZO CONGENITO",
        "GES MARCAPASO",
        "GES MARCAPASO PRE QUIRÚRGICO",
        "DESFIBRILADORES / RESINCRONIZADORES",
        "ELECTROFISIOLOGIA",
    ],
    "Cuidados Paliativos": ["GES CUIDADOS PALIATIVOS"],
}


@decorador_tiempo
def leer_his(input_filepath):
    """
    Reads and preprocesses diagnostic data from multiple Excel files.

    :param input_filepath: The path to the input directory.
    :type input_filepath: str

    :return: The preprocessed diagnostic DataFrame.
    :rtype: pandas DataFrame
    """
    # Lee la base de datos
    print("> Leyendo la base de datos HIS")
    df = pd.concat(
        (
            pd.read_excel(archivo, usecols=COLUMNAS_UTILES_HIS)
            for archivo in glob.glob(f"{input_filepath}/his/*.xlsx")
        )
    )

    # Limpia el nombre de las columnas
    df = clean_column_names(df)

    # Limpia la columna de sexo
    df["sexo"] = preprocesar_sexo(df["sexo"])

    # Convierte la fecha de nacimiento a datetime
    df["fecha_nacimiento"] = pd.to_datetime(df["fecha_nacimiento"], dayfirst=True, errors="coerce")

    # Define las columnas del DataFrame y elimina la columna que no se repite (Detalle Atencion)
    columnas_repetidas = list(df.columns)
    columna_no_repetida = "detalle_atencion"
    columnas_repetidas.remove(columna_no_repetida)

    # Convierte la columna sin repetir a string y une el DataFrame
    df[columna_no_repetida] = df[columna_no_repetida].astype(str)
    df = unir_filas_repetidas(df, columnas_repetidas, columna_no_repetida)

    # Procesa los RUTs
    # df = salted_sha256_anonymize(df, COLS_A_HASHEAR)
    df = df.rename(columns={"rut_paciente": "id_paciente"})

    # Preprocesa los codigos de diagnosticos de la base de datos
    df["codigo_diagnostico"] = preprocesar_diagnostico(df["codigo_diagnostico"].astype(str))
    diccionario_diagnosticos = obtener_diccionario_traductor_diags()
    df["codigo_diagnostico"] = df["codigo_diagnostico"].replace(diccionario_diagnosticos)
    df = modificar_diags_largo_3(df)
    df = modificar_diags_largo_5(df)

    # Agrega la columna de especialidad agrupada
    df = agrupar_especialidades(df)

    return df


def preprocesar_diagnostico(serie_diagnostico):
    """
    Preprocesses a series of diagnoses by removing special characters.

    :param serie_diagnostico: The series of diagnoses.
    :type serie_diagnostico: pandas Series

    :return: The preprocessed series of diagnoses.
    :rtype: pandas Series
    """
    return serie_diagnostico.str.replace("\.|\s|_", "", regex=True)


def preprocesar_sexo(serie_sexo):
    """
    Preprocesses a series of genders by removing leading/trailing whitespaces and
    converting to lowercase.

    :param serie_sexo: The series of genders.
    :type serie_sexo: pandas Series

    :return: The preprocessed series of genders.
    :rtype: pandas Series
    """
    return serie_sexo.str.strip().str.lower()


def unir_filas_repetidas(df, columnas_repetidas, columna_distinta):
    """
    Concatenates rows with duplicate values in specified columns, joining the distinct
    values in another column.

    :param df: The input DataFrame.
    :type df: pandas DataFrame
    :param columnas_repetidas: A list of column names with duplicate values.
    :type columnas_repetidas: list
    :param columna_distinta: The column containing distinct values to be joined.
    :type columna_distinta: str

    :return: The DataFrame with concatenated rows.
    :rtype: pandas DataFrame
    """
    if df.empty:
        return pd.DataFrame()

    tmp = (
        df.groupby(columnas_repetidas, dropna=False)[columna_distinta]
        .apply(", ".join)
        .reset_index()
    )
    return tmp


def obtener_diccionario_traductor_diags():
    archivo = pd.ExcelFile("data/external/diagnosticos_encontrados_asignados_cie_10.xlsx")
    df = pd.concat((pd.read_excel(archivo, sheet_name=hoja) for hoja in archivo.sheet_names))

    diagnosticos_anomalos_con_cie = df[~df["CIE 10"].isna()][["codigo_diagnostico", "CIE 10"]]
    diagnosticos_anomalos_con_cie["codigo_diagnostico"] = diagnosticos_anomalos_con_cie[
        "codigo_diagnostico"
    ].astype(str)
    diccionario_diags = diagnosticos_anomalos_con_cie.set_index("codigo_diagnostico").to_dict()[
        "CIE 10"
    ]

    return diccionario_diags


def modificar_diags_largo_3(df):
    tmp = df.copy()

    mask_largo_3 = tmp["codigo_diagnostico"].str.len() == 3
    tmp.loc[mask_largo_3, "codigo_diagnostico"] = tmp.loc[
        mask_largo_3, "codigo_diagnostico"
    ].str.ljust(4, "X")

    return tmp


def modificar_diags_largo_5(df):
    tmp = df.copy()

    mask_largo_5_con_guion = (tmp["codigo_diagnostico"].str.len() == 5) & ~(
        tmp["codigo_diagnostico"].str.contains("-")
    )

    tmp.loc[mask_largo_5_con_guion, "codigo_diagnostico"] = tmp.loc[
        mask_largo_5_con_guion, "codigo_diagnostico"
    ].str[:4]

    return tmp


def agrupar_especialidades(df):
    # Agrega columna para agrupar por distintas especialidades internas a las REM
    tmp = df.copy()
    tmp["especialidad_agrupada"] = tmp["nombre_especialidad"]

    # Reasigna especialidades
    for nuevo_valor, a_cambiar in AGRUPACIONES_ESPECIALIDAD.items():
        tmp["especialidad_agrupada"] = tmp["especialidad_agrupada"].replace(a_cambiar, nuevo_valor)

    return tmp


if __name__ == "__main__":
    df_his = leer_his("data/raw")
    df_his.to_csv("data/processed/his_procesada.csv", index=False)
