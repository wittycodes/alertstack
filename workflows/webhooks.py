# https://www.restack.io/docs/airflow-knowledge-apache-webhook-connect-rest-api-providers-http-pypi

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2021, 1, 1)
}

with DAG('example_http_operator', default_args=default_args, schedule_interval=None) as dag:
    task = SimpleHttpOperator(
        task_id='post_op',
        http_conn_id='http_default',
        endpoint='api/v1/external/trigger',
        method='POST',
        data='{"dag_run_id": "my_custom_run_id"}',
        headers={"Content-Type": "application/json"}
    )