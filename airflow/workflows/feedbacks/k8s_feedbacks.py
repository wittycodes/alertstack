import logging
import pickle
from airflow.decorators import task
logger = logging.getLogger(__name__)

from kubernetes import client, config

config.load_incluster_config()

v1 = client.CoreV1Api()
apps_v1 = client.AppsV1Api()


# List pods
@task
def list_pods(ti):
    ns = ti.xcom_pull(key="namespace", task_ids='get_alerts')
    pods = v1.list_namespaced_pod(namespace=ns)
    for i in pods.items:
        print(i.metadata.name)
        logger.info(i.metadata.name)
        logger.info("This is an info message")
        logger.warning("This is a warning message")
        logger.error("This is an error message")
    ti.xcom_push(key='pods', value=pickle.dumps(pods))
    return pods


@task
def get_pod_volume(namespace):
    return None
