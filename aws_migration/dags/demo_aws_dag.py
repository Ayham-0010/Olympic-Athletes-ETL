# airflow/dags/olympic_demo_dag.py
from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="olympic_demo_etl",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,          # manual trigger for demo
    catchup=False,
    default_args=default_args,
    tags=["demo", "glue", "localstack"],
) as dag:

    run_glue_demo = GlueJobOperator(
        task_id="run_glue_demo_job",
        job_name="demo_job_localstack",
        script_location="s3://bronze/scripts/glue_demo_job.py",
        script_args={"--JOB_NAME": "demo_job_localstack"},
        s3_bucket="bronze",
        aws_conn_id="localstack_conn",
        region_name="us-east-1",
    )