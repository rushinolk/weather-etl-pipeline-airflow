import pendulum
from datetime import timedelta
from clima_pipeline.clima_pipeline import (extract_clima_data, transform_data, load_clima_data)
from airflow import DAG
from airflow.sdk import task
from airflow.utils.task_group import TaskGroup
from airflow.providers.common.sql.operators.sql import SQLTableCheckOperator, SQLColumnCheckOperator

POSTGRES_CONN_ID = 'clima_db'

default_args = {
    "depends_on_past" : False,
    "email": ["teste@email.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
}


with DAG(
    dag_id="dag_clima_pipeline",
    description="Orquestração do projeto de ETL da API Clima",
    default_args=default_args,
    schedule=None,
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    catchup=False,
    tags=["Pratica","Etl"]
) as dag:
    
    with TaskGroup(group_id='Processamento_ETL') as tsk_group_data:
        @task(task_id='extract_data')
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



    with TaskGroup(group_id='Verificando_dados') as tsk_group_check:
        chk_vendas_tem_dados = SQLTableCheckOperator(
            task_id='chk_clima_tem_dados',
            conn_id = POSTGRES_CONN_ID,
            table='previsao_clima',
            checks={"row_count_nonzero": {"check_statement": "COUNT(*)>0"}},
        )

        chk_colunas_vendas = SQLColumnCheckOperator(
            task_id='chk_coluna_data',
            conn_id = POSTGRES_CONN_ID,
            table='previsao_clima',
            column_mapping={
                "data_previsao": {"null_check": {"equal_to": 0}}                      
            },
        )

    
    tsk_group_data >> [tsk_group_check]






     