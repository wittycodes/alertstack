import logging

from airflow.decorators import task
from utils.decorators import xcom_push
logger = logging.getLogger(__name__)

from kubernetes import client, config

config.load_incluster_config()

v1 = client.CoreV1Api()
apps_v1 = client.AppsV1Api()


# List pods
@xcom_push(key='pods')
@task
def list_pods(namespace):
    pods = v1.list_namespaced_pod(namespace=namespace)
    for i in pods.items:
        print(i.metadata.name)
        logger.info(i.metadata.name)
        logger.info("This is an info message")
        logger.warning("This is a warning message")
        logger.error("This is an error message")
    return pods


@task
def get_pod_volume(namespace):
    return None
