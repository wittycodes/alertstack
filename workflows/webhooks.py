# https://www.restack.io/docs/airflow-knowledge-apache-webhook-connect-rest-api-providers-http-pypi

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime
import logging
logger = logging.getLogger(__name__)



from kubernetes import client, config
config.load_incluster_config()

v1 = client.CoreV1Api()
apps_v1 = client.AppsV1Api()



# List pods
def list_pods(namespace):
    pods = v1.list_namespaced_pod(namespace=namespace)
    for i in pods.items:
        print(i.metadata.name)
        logger.info(i.metadata.name)
        logger.info("This is an info message")
        logger.warning("This is a warning message")
        logger.error("This is an error message")


default_args = {
    'start_date': datetime(2021, 1, 1)
}

with DAG('webhook_http_operator', default_args=default_args, schedule_interval=None) as dag:

    list_pods_1 = PythonOperator(
        task_id='list_pods_1',
        python_callable=list_pods,
        op_args=['monitoring'],
        dag=dag
    )

    list_pods_2 = PythonOperator(
        task_id='list_pods_2',
        python_callable=list_pods,
        op_args=['prometheus'],
        dag=dag
    )

    list_pods_1 >> list_pods_2


    # task = SimpleHttpOperator(
    #     task_id='post_op',
    #     http_conn_id='http_default',
    #     endpoint='api/v1/external/trigger',
    #     method='POST',
    #     data='{"dag_run_id": "my_custom_run_id"}',
    #     headers={"Content-Type": "application/json"}
    # )