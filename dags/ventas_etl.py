import pandas as pd
from airflow.decorators import dag, task
from datetime import datetime
from pathlib import Path
from sklearn.preprocessing import MinMaxScaler,LabelEncoder
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
    def leer_archivo_ventas() -> pd.DataFrame:
        df_ventas=pd.read_excel(os.path.join(data_dir,"bd_ventas.xlsx"),sheet_name=0)
        return df_ventas
    
    @task
    def leer_archivo_vehiculos() -> pd.DataFrame:
        df_vehiculos= pd.read_excel(os.path.join(data_dir,"bd_ventas.xlsx"),sheet_name=1)
        return df_vehiculos
    
    @task 
    def procesar_dataframe(df_ventas:pd.DataFrame,df_vehiculos:pd.DataFrame) -> pd.DataFrame:
        #CALCULO DEL COSTO ORIGINAL DEL VEHÍCULO
        df_ventas['Precio Venta sin IGV'] = pd.to_numeric(df_ventas['Precio Venta sin IGV'], errors='coerce')
        df_ventas['Costo Vehiculo'] = 0.6 * df_ventas['Precio Venta sin IGV']
        #CALCULO DEL PRECIO DE VENTA REAL (MÁS IGV)
        df_ventas['Precio Venta Real'] = df_ventas['Precio Venta sin IGV'] + 0.18 * df_ventas['Precio Venta sin IGV']
        #OBTENCIÓN DE LA SEDE
        df_ventas[['Distrito', 'Provincia', 'Departamento']] = df_ventas['Ubicación'].str.split(',', expand=True)
        df_ventas=df_ventas.rename(columns={'Distrito': 'Sede'}) #Renombre de la columna a sede
        #RELIZAMOS UN JOIN DE LOS DF VENTAS Y VEHICULOS
        df_ventas = pd.merge(df_ventas, df_vehiculos,left_on='ID_Vehículo',right_on='ID_Vehiculo', how='inner')
        df= df_ventas.drop(columns=['Ubicación', 'ID_Vehículo','ID_Vehiculo','Provincia','Departamento','Cliente','MARCA','MODELO','Vendedor'])
        df.set_index('ID', inplace=True)
        #NORMALIZACIÓN DE VALORES DE VENTA Y COSTO ()
        scaler = MinMaxScaler()
        df['Precio Venta sin IGV'] = scaler.fit_transform(df[['Precio Venta sin IGV']])
        df['Costo Vehiculo'] = scaler.fit_transform(df[['Costo Vehiculo']])
        df['Precio Venta Real'] = scaler.fit_transform(df[['Precio Venta Real']])
        #TRANSFORMACIÓN LINEAL PARA EL AÑO DE FRABRICACIÓN DEL VEHÍCULO
        le = LabelEncoder()
        df['AÑO'] = le.fit_transform(df['AÑO'])
        df=df.rename(columns={'AÑO': 'ano_fabricacion'}) #Renombre de la columna
        #ONE HOT ENCODING SEGUN TIPO DE VEHICULO, SEGMENTO Y SEDE
        df_encoded_tv = pd.get_dummies(df.TIPO_VEHÍCULO, dtype=int)
        df_encoded_seg = pd.get_dummies(df.Segmento, dtype=int)
        df_encoded_sede = pd.get_dummies(df.Sede, dtype=int)
        df=pd.concat([df,df_encoded_tv],axis=1)
        df=pd.concat([df,df_encoded_seg],axis=1)
        df=pd.concat([df,df_encoded_sede],axis=1)
        df= df.drop(columns=['Sede','Segmento', 'TIPO_VEHÍCULO'])
        #LIMPIEZA DE DATOS DE PRECIO
        #Debido a que los precios venta sin igv, costo vehiculo y precio venta real son proporcionales. Cuando se realiza la normalización nos da la misma información , por tanto se procede a la eliminación de las columnas
        df= df.drop(columns=['Precio Venta sin IGV','Precio Venta Real'])
        df=df.rename(columns={'Costo Vehiculo': 'Costo'}) #Renombre de la columna
        #TARGET ENCODING PARA EL CANAL, USANDO COMO BASE LA MEDIA DEL COSTO
        mean_map = df.groupby('Canal')['Costo'].mean()
        df['canal_encoded'] = df['Canal'].map(mean_map)
        df= df.drop(columns=['Canal'])
        # Convertir la columna a formato datetime
        df['Fecha'] = pd.to_datetime(df['Fecha'])
        # Extraer Mes y Año en columnas nuevas
        df['Mes'] = df['Fecha'].dt.month
        df['Año'] = df['Fecha'].dt.year
        # Eliminar la columna original
        df.drop(columns=['Fecha'], inplace=True)
        # Renombrar columnas
        return df
    


    @task 
    def renombrar_columnas(df:pd.DataFrame) -> pd.DataFrame:
        df.rename(columns={'Costo': 'costo', 'AUTO': 'auto','CAMIÓN':'camion','Año':'ano','Mes':'mes','AUTOBUS':'autobus','CAMIONETA':'camioneta','Empresa':'empresa','Persona':'persona'}, inplace=True)
        df.rename(columns={'San Miguel': 'san_miguel'}, inplace=True)
        df.rename(columns={'Surco': 'surco'}, inplace=True)
        df.rename(columns={'Ate': 'ate'}, inplace=True)
        df.rename(columns={'La Molina': 'la_molina'}, inplace=True)
        return df



    @task 
    def guardar_dataframe(df:pd.DataFrame):
        import sqlalchemy as sa
        engine = sa.create_engine("postgresql+psycopg2://demo:demo@warehouse-db:5432/warehouse")
        with engine.begin() as conn:
            df.to_sql("ventas_procesado", conn, if_exists="append", index=False)
        

    df_ventas= leer_archivo_ventas()
    df_vehiculos= leer_archivo_vehiculos()
    df1=procesar_dataframe(df_ventas,df_vehiculos)
    df2=renombrar_columnas(df1)
    guardar_dataframe(df2)



dag = ventas_etl()
