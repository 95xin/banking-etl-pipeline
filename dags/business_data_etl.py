from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="banking_etl_pipeline",
    start_date=datetime(2025,1,1),
    schedule_interval="@daily",
    catchup=False,
    default_args={"owner":"airflow","retries":1,"retry_delay":timedelta(minutes=2)},
) as dag:

    ingestion = BashOperator(
        task_id="ingestion_s3_to_postgres",
        bash_command="python /opt/airflow/spark/data_ingestion.py",
    )

    transformation = SparkSubmitOperator(
        task_id="transformation_spark_to_s3",
        conn_id="spark-conn",
        application="/opt/airflow/spark/data_transformation.py",
        # 如果你的 Spark 镜像没内置依赖，再打开 packages：
        packages=(
            "org.postgresql:postgresql:42.7.4,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.772"
        ),  
        verbose=True,
    )

    ingestion >> transformation