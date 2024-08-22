# Explicación Bases de Datos Ambulatorias

## HIS

- Esta base de datos indica todas las consultas que fueron atendidas.

- En el sistema HIS es donde se evoluciona al paciente, y se indica si es que este fue atendido o no. Se conecta con el sistema TrackCare para indicar en esta última las consultas que efectivamente fueron atendidas.

## TrackCare

- Es la base de datos que lleva la agenda de consultas ambulatorias. Posee las consultas Atendidas (reportado por HIS al terminar el día), Canceladas, Transferidas, No Atendidas, etc.

- **De esta base de datos se obtiene el conteo de consultas para REM**. Para contabilizarlas, solamente dejan las consultas Atendidas (proporcionado por HIS), y realizadas por un medico/psiquiatra. TrackCare ya posee la diferenciacion por especialidad.

- La producción utilizada para la plataforma SIGCOM se obtiene desde esta base de datos. Por lo tanto, coincide al 100% con REM.

Teóricamente, HIS y TrackCare debiesen coincidir siempre en las consultas atendidas. Esto, ya que HIS proporciona la información devuelta hacia TrackCare sobre tal tipo de consultas.

## Datos Estadísticos

- Es la base de datos que lleva la contabilización de procedimientos tanto ambulatorios como de hospitalizados. Estos datos son ingresados por los operadores hacia la plataforma HIS, y posteriormente son extraidos desde allí.
