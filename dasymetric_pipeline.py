from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "geo-team",
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}

with DAG("dasymetric_pipeline", default_args=default_args, schedule_interval="@monthly", catchup=False) as dag:

    run_processing = BashOperator(
        task_id="run_processing",
        bash_command="docker run --rm -v /data:/app/data -v /output:/app/output dasymetric-population:latest"
    )

    upload_s3 = BashOperator(
        task_id="upload_s3",
        bash_command="python /app/scripts/s3_uploader.py"
    )

    update_stac = BashOperator(
        task_id="update_stac",
        bash_command="python /app/scripts/stac_uploader.py"
    )

    run_processing >> upload_s3 >> update_stac
