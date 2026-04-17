# TRABAJO COLABORATIVO

Curso: Producto académico colaborativo
Versión: 1.0.1

## Alumnos

* Alcantara Rivera Jose Alexander
* Alfaro Gutierrez Dermy Edsel
* Ancalle Gonzales Fabio Steve

## Ejecución
 
```bash
cd airflow-poc
docker compose up airflow-init
docker compose up -d
# UI: http://localhost:8080  (admin / admin)
```

## Airflow Task

Frecuencia: Cada hora

Se listan las tareas(tasks) del DAG "ventas_etl"

- **leer_archivo_ventas**: Se lee la página 0 del archivo "db_ventas.xlsx" y devuelve el dataframe de ventas
- **leer_archivo_vehiculos**: Se lee la página 1 del archivo "db_ventas.xlsx" y devuelve el dataframe de vehículos
- **procesar_dataframe**: Realiza las trasnformaciones de la información y devuelve el dataframe transformado
- **renombrar_columnas**: Renombra los encabezados de ciertas columnas
- **guardar_dataframe**: Guarda el dataframe en la base de datos de postgresql y las guarda en la tabla "ventas_procesado"

## Limpieza 

```bash
docker compose down -v
```
