import pendulum
from clima_pipeline.clima_pipeline import (extract_clima_data, transform_data, load_clima_data)
from airflow import DAG
from airflow.sdk import task


with DAG(
    dag_id="dag_clima_pipeline",
    description="Orquestração do projeto de ETL da API Clima",
    schedule=None,
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    catchup=False,
    tags=["Pratica","Etl"]
) as dag:
    
    @task(task_id='extract_clima_data')
    def task_extract():
        return extract_clima_data()

    @task(task_id='transform_data')
    def task_transform(dados_brutos):
        return transform_data(dados_brutos)

    @task(task_id='load_data')
    def task_load(dados_transformados):
        return load_clima_data(dados_transformados)


    dados_brutos = task_extract()
    dados_tansformados = task_transform(dados_brutos)
    task_load(dados_tansformados)