from datetime import datetime, timedelta
from airflow import DAG
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator, DataprocDeleteBatchOperator,
    DataprocGetBatchOperator, DataprocListBatchesOperator
)
from airflow.utils.dates import days_ago

PYTHON_FILE_LOCATION = "gs://hiveconnect_test_spk/scripts/seq_files/avrofile1.py"
# SPARK_BIGQUERY_JAR_FILE = "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
SPARK_AVRO_JAR_FILE = "gs://hiveconnect_test_spk/scripts/seq_files/spark-avro_2.12-3.3.1.jar"
CLUSTER_PATH = "projects/hca-dei-sbx/regions/us-central1/clusters/tmp-hc"
#    "projects/{{ var.value.project_id }}/regions/{{ var.value.region_name}}/clusters/{{ var.value.phs_cluster }}"
OUTPUT_FOLDER = "gs://hiveconnect_test_spk/scripts/seq_files/test_output_{}".format(datetime.now().strftime('%Y%m%d%H%M%S'))

default_args = {
    # Tell airflow to start one day ago, so that it runs as soon as you upload it
    'owner': 'airflow',
    "start_date": datetime(2023, 1, 1),
    "project_id": 'hca-dei-sbx',
    "region": 'us-central1',
    'depends_on_past': False,
    "retries": 2,
    "retry_delay": timedelta(seconds=10)
}

with models.DAG(
    "dataproc_batch_operators",  # The id you will see in the DAG airflow page
    default_args=default_args,  # The interval with which to schedule the DAG
    schedule_interval=None,  # Override to match your needs
) as dag:

    create_batch = DataprocCreateBatchOperator(
        task_id="batch_create",
        batch_id="batch-create-hc-{}".format(datetime.now().strftime('%Y%m%d%H%M%S')),
        batch={
            "pyspark_batch": {
                "main_python_file_uri": PYTHON_FILE_LOCATION,
                "args": ["gs://hiveconnect_test_spk/scripts/seq_files/test_input","hca-dei-sbx.hcs_poc.avrotable",OUTPUT_FOLDER],
                "jar_file_uris": [SPARK_AVRO_JAR_FILE],
            },
            "environment_config": {
                "execution_config": {"subnetwork_uri": "https://www.googleapis.com/compute/v1/projects/hca-dei-sbx/regions/us-central1/subnetworks/bigtablepoc"}
                },
        },
    )
    
    create_batch



