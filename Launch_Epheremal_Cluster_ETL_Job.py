import os

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.dates import days_ago

PROJECT_ID = 'bustling-surf-310323'
REGION = 'us-east1'
CLUSTER_NAME = 'ephemeral-spark-cluster-{{ ds_nodash }}'
PYSPARK_URI = 'gs://BUCKET_DE_DAG/AirQuality_ETL_Job.py'

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI,
    "jar_file_uris":["gs://spark-lib/bigquery/spark-3.3-bigquery-0.36.1.jar"]
    }
}

cluster_config_json = {
    "worker_config": {
      "num_instances": 2
    }
}

args = {
    'owner': 'Axel Alemán',
}

with DAG(
    dag_id='dataproc_ephemeral_cluster_job',
    schedule_interval='0 5 * * *',
    start_date=days_ago(1),
    default_args=args
) as dag:
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=cluster_config_json,
        region=REGION,
        cluster_name=CLUSTER_NAME
    )

    pyspark_task = DataprocSubmitJobOperator(
        task_id="pyspark_task", job=PYSPARK_JOB, region=REGION, project_id=PROJECT_ID
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster", project_id=PROJECT_ID, cluster_name=CLUSTER_NAME, region=REGION
    )

create_cluster >> pyspark_task >> delete_cluster