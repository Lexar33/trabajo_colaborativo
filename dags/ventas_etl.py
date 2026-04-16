import pandas as pd
from airflow.decorators import dag, task
from datetime import datetime
from pathlib import Path
import os


@dag(
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["poc", "taskflow", "dynamic-mapping"],
)
def ventas_etl():

    data_dir = Path("/opt/airflow/data")
    
    @task 
    def leer_archivo() -> pd.DataFrame:
        df=pd.read_excel(os.path.join(data_dir,"bd_ventas.xlsx"))
        return df 
        
    @task 
    def procesar_dataframe(df:pd.DataFrame) :
        print(f"Filas procesadas: {len(df)}")
        return "df.to_csv"


    df= leer_archivo()
    procesar_dataframe(df)


dag = ventas_etl()
