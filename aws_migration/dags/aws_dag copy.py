# airflow/dags/olympic_demo_dag.py
from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount



default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="aws_demo_etl_glue5_",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,          # manual trigger for demo
    catchup=False,
    default_args=default_args,
    tags=["glue", "localstack"],
) as dag:

    run_glue5_job = DockerOperator(
        task_id="run_glue5_spark_submit",
        image="aws-glue-5-extended:1.0", # public.ecr.aws/glue/aws-glue-libs:5
        command=[
            "spark-submit",
            "--conf", "spark.hadoop.fs.s3a.endpoint=http://localstack:4566",
            "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
            "/home/hadoop/workspace/src/athlete_scrape_pyspark.py",
            "--JOB_NAME", "demo_job"
        ],
        environment={
            "AWS_ACCESS_KEY_ID": "test",
            "AWS_SECRET_ACCESS_KEY": "test",
            "AWS_DEFAULT_REGION": "us-east-1"
        },
        docker_url="unix://var/run/docker.sock",
        network_mode="aws-net",
        mount_tmp_dir=False,
        mounts=[
            Mount(source="/home/ayham/Portfolio/Olympic-Athletes-ETL/aws_migration/.aws", target="/home/hadoop/.aws", type="bind"),
            Mount(source="/home/ayham/Portfolio/Olympic-Athletes-ETL/aws_migration", target="/home/hadoop/workspace", type="bind"),
        ],
        auto_remove="force",
        tty=True,
    )
# "pip install pandas beautifulsoup4 lxml requests", "&&", # install dependencies every time container starts (buiding custom image in the future)