# https://www.restack.io/docs/airflow-knowledge-apache-webhook-connect-rest-api-providers-http-pypi
from airflow.decorators import task, dag, branch_task
import enrichment.k8s.pod
import enrichment.aws.vm
import remediation.k8s.pod
import dags.k8s.pod_scenarios
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

default_args = {
    'start_date': datetime(2021, 1, 1)
}


@task
def get_alerts(ti):
    data = ti.xcom_push(key="namespace", value="prometheus")
    ti.xcom_push(key="alerts", value=ti.dag_run.conf)
    logger.info("it's good inside webhook_prometheus_entrypoint")
    logger.info(data)


@branch_task
def choose_scenarios(ti):
    data = ti.xcom_pull(key="alerts")
    ti.xcom_push(key="scenarios", value=data)
    x = 1
    if(x==1):
        return ['list_pods', 'get_pod_volume']
    else:
        return ['get_cpu']

@dag(dag_id='webhook_prometheus_entrypoint', default_args=default_args, schedule_interval=None)
def webhook_prometheus_entrypoint():
    get_alerts() >> choose_scenarios() >> [ enrichment.k8s.pod.list_pods(), enrichment.k8s.pod.get_pod_volume(), enrichment.aws.vm.get_cpu()]


webhook_prometheus_entrypoint()
