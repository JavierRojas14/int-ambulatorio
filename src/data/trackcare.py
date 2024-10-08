import glob

import pandas as pd
from funciones_auxiliares import (
    clean_column_names,
    decorador_tiempo,
    unificar_formato_ruts,
)

COLUMNAS_UTILES_TRACKCARE = [
    "PAPMIID",
    "Sexo",
    "Edad",
    "Comuna",
    "Provincia",
    "Region",
    "Prevision",
    "Plan",
    "Prestacion",
    "TipoAtencion",
    "FechaCita",
    "HoraCita",
    "EstadoCita",
    "Especialidad",
    "TipoProfesional",
]

CAMBIO_COMUNA_TRACKCARE = {
    "Maipú": "MAIPU",
    "Chillán": "CHILLAN",
    "Chépica": "CHEPICA",
    "Concón": "VALPARAISO",
    "Copiapó": "COPIAPO",
    "Curicó": "CURICO",
    "Los Angeles": "LOS ÁNGELES",
    "Quilpué": "QUILPUE",
    "Valparaíso": "VALPARAISO",
    "María Elena": "MARIA ELENA",
    "Santa María": "SANTA MARIA",
    "Chillán Viejo": "CHILLAN VIEJO",
    "Tomé": "TOME",
}

GLOSAS_CONSULTAS_REPETIDAS = [
    "CR - Control Ges Epoc",
    "CR - Control Ges Asma",
    "CR-Atención presencial",
    "CR- Atencion Email",
    "Consulta Compleja Repetida",
    "C. Repetida Atención Telefónica",
    "C. Repetida Telemedicina",
]

GLOSAS_CONSULTAS_NUEVAS = [
    "CN - Post Operado",
    "CN - Consulta nueva GES",
    "CN - 15 días",
    "CN - Ingreso de Pacientes",
    "CN - Post Examen",
    "CN - Rehabilitacion Pulmonar",
    "CN - Post Operados",
    "CN-Atención presencial",
    "CN - Atencion Inmediata",
    "CN Telemedicina",
    "CN- Atencion Email",
    "CN - Control prioritario",
    "C. Nueva Atención Telefónica",
    "C. Nueva Telemedicina",
]

GLOSAS_CONSULTAS_PROCEDIMIENTOS = [
    "PROC - Toma de Muestra",
    "PROC - KTR Integral",
    "PROC - Test Ejercicios",
]

GLOSAS_CONSULTAS_MISCALENEAS = ["Consulta Abreviada", "Control Post Operado"]

AGRUPACIONES_ESPECIALIDAD = {
    "ANESTESIOLOGÍA": [],
    "CARDIOLOGÍA": ["Cardiología", "Cardiología Adulto"],
    "CIRUGÍA CARDIOVASCULAR": [
        "Cardiocirugía",
        "Cardiocirugía Adulto",
        "Cirugía Adulto",
        "Cirugía General",
        "Cirugía Vascular Periférica",
    ],
    "CIRUGÍA DE TÓRAX": ["Cirugía Tórax"],
    "ENFERMEDADES RESPIRATORIAS ADULTO": ["Broncopulmonar", "Broncopulmonar Adulto"],
    "ENFERMERÍA": [],
    "EQUIPO MULTIDISCIPLINARIO": [],
    "GENÉTICA CLÍNICA": [],
    "INFECTOLOGÍA": ["Infectología Adulto"],
    "MEDICINA FÍSICA Y REHABILITACIÓN": [],
    "MEDICINA INTERNA": ["Med. Interna"],
    "NUTRICIÓN": ["Nutrición"],
    "ODONTOLOGÍA": [],
    "ONCOLOGÍA": ["Oncología"],
    "PALIATIVISTA": [],
    "PSICOLOGÍA": ["Psicología"],
    "PSIQUIATRÍA ADULTO": ["Psiquiatría Adulto"],
    "QUÍMICA Y FARMACIA": [],
    "RADIOTERAPEUTA": [],
    "TRABAJO SOCIAL": [],
}


@decorador_tiempo
def leer_trackcare(input_filepath):
    print("> Leyendo TrackCare")
    # Lee las bases de datos
    ruta_archivos = f"{input_filepath}/trackcare/*.xls"
    df = pd.concat(
        (
            pd.read_csv(archivo, encoding="latin-1", sep="\t", usecols=COLUMNAS_UTILES_TRACKCARE)
            for archivo in glob.glob(ruta_archivos)
        )
    )

    # Limpia el nombre de las columnas
    df = clean_column_names(df)

    # Agrega una columna de la fecha y hora de la atencion
    df["hora_completa_cita"] = pd.to_datetime(df["fechacita"] + " " + df["horacita"], dayfirst=True)

    # Agrega columna de anio
    df["ano"] = df["hora_completa_cita"].dt.year

    # Formatea el id del paciente
    df["id_paciente"] = unificar_formato_ruts(df["papmiid"], eliminar_digito_verificador=True)

    # Agrega una columna del rangio etario
    df["rango_etario"] = pd.cut(df.edad, bins=range(0, 151, 10))

    # Reemplaza valores en las columnas comuna y tipoatencion
    df["comuna"] = df["comuna"].replace(CAMBIO_COMUNA_TRACKCARE)
    df["tipoatencion"] = (
        df["tipoatencion"]
        .replace(GLOSAS_CONSULTAS_NUEVAS, "Consulta Nueva")
        .replace(GLOSAS_CONSULTAS_REPETIDAS, "Consulta Repetida")
        .replace(GLOSAS_CONSULTAS_PROCEDIMIENTOS, "Procedimiento")
        .replace(GLOSAS_CONSULTAS_MISCALENEAS, "Miscaleneo")
    )

    # Agrega columna de especialidades renombradas
    df["especialidad"] = df["especialidad"].str.strip()
    df = agrupar_especialidades(df)

    # Elimina el RUT de la persona
    df = df.drop(columns=["papmiid"])

    return df


def agrupar_especialidades(df):
    # Agrega columna para agrupar por distintas especialidades internas a las REM
    tmp = df.copy()
    tmp["especialidad_agrupada"] = tmp["especialidad"]

    # Reasigna especialidades
    for nuevo_valor, a_cambiar in AGRUPACIONES_ESPECIALIDAD.items():
        tmp["especialidad_agrupada"] = tmp["especialidad_agrupada"].replace(a_cambiar, nuevo_valor)

    return tmp


if __name__ == "__main__":
    df = leer_trackcare("data/raw")
    df.to_csv("data/processed/trackcare_procesada.csv", index=False)
