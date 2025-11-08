from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta


from src.data_clean import data_clean_I
from src.data_clean_II import data_clean_II
from src.data_quality_and_validation import data_validation_quality_checks
from src.columns_renaming_reordering_and_final_save import column_rename_reorder
from src.athlete_scrape import scrap_athletes
from src.editions_scrap import scrap_editions

default_args = {

    'owner': 'ayham',
    'retries':5,
    'retry_delay':timedelta(minutes=2)


}


with DAG (

    dag_id='Olympic_Athletes_ETL_V1',
    description='practice',
    default_args=default_args,
    start_date=datetime(2025,11,8),
    schedule_interval='@daily'



)as dag:
    

    # scrap_athletes_task=PythonOperator(

    # task_id = "scrap_athletes",
    # python_callable=scrap_athletes

    # )

    # scrap_editions_task=PythonOperator(

    # task_id = "scrap_editions",
    # python_callable=scrap_editions

    # )

    data_clean_I_task=PythonOperator(

        task_id = "data_clean_I",
        python_callable=data_clean_I

    )
    data_clean_II_task=PythonOperator(

        task_id = "data_clean_II",
        python_callable=data_clean_II

    )

    column_rename_reorder_task=PythonOperator(

        task_id = "column_rename_reorder",
        python_callable=column_rename_reorder

    )

    data_validation_quality_checks_task=PythonOperator(

        task_id = "data_validation_quality_checks",
        python_callable=data_validation_quality_checks

    )


# scrap_athletes_task \
# >> scrap_editions_task \
# >> 
data_clean_I_task \
>> data_clean_II_task \
>> data_validation_quality_checks_task \
>> column_rename_reorder_task


