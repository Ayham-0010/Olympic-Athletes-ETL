from airflow import DAG
from airflow.operators.python import PythonOperator


from datetime import datetime, timedelta

from src.data_clean import data_clean_I


default_args = {

    'owner': 'ayham',
    'retries':5,
    'retry_delay':timedelta(minutes=2)


}


with DAG (

    dag_id='Olympic_Athletes_ETL_DAG_V1',
    description='practice',
    default_args=default_args,
    start_date=datetime(2025,11,7),
    schedule_interval='@daily'



)as dag:
    task1=PythonOperator(

        task_id = "data_clean_I",
        python_callable=data_clean_I

    )

