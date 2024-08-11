# https://www.restack.io/docs/airflow-knowledge-apache-webhook-connect-rest-api-providers-http-pypi

from airflow.decorators import task, dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from notifiers import slack, pagerduty, customemail, discord
from actions import k8s_actions
from feedbacks import k8s_feedbacks

from datetime import datetime
import logging

logger = logging.getLogger(__name__)

default_args = {
    'start_date': datetime(2021, 1, 1)
}


@task
def get_alerts(**kwargs):
    data = kwargs['ti'].xcom_push(task_ids='get_alerts', )
    logger.info("it's good inside webhook_prometheus_entrypoint")
    logger.info(data)


@dag(dag_id='webhook_prometheus_entrypoint')
def webhook_prometheus_entrypoint():
    pod_volume_analysis_trigger_dag = TriggerDagRunOperator(
        task_id='pod_volume_analysis_trigger_dag',
        trigger_dag_id='pod_volume_analysis',
        # conf={'my_param': 'value'},
        wait_for_completion=False,
    )

    pod_memory_analysis_trigger_dag = TriggerDagRunOperator(
        task_id='pod_memory_analysis_trigger_dag',
        trigger_dag_id='pod_memory_analysis',
        # conf={'my_param': 'value'},
        wait_for_completion=False,
    )

    node_cpu_analysis_trigger_dag = TriggerDagRunOperator(
        task_id='node_cpu_analysis_trigger_dag',
        trigger_dag_id='node_cpu_analysis',
        # conf={'my_param': 'value'},
        wait_for_completion=False,
    )

    get_alerts()
    # pod_volume_analysis_trigger_dag >> [ node_cpu_analysis_trigger_dag, pod_memory_analysis_trigger_dag]


@dag(dag_id='pod_volume_analysis')
def pod_volume_analysis():
    list_pods_1 = k8s_feedbacks.list_pods("monitoring")

    list_pods_2 = k8s_feedbacks.list_pods("prometheus")

    query_pod_volume = k8s_feedbacks.get_pod_volume("prometheus")

    action = k8s_actions.increase_pod_volume("monitoring")

    final_call = [
        slack.send_to_slack("hello"),
        pagerduty.send_to_pagerduty("hello"),
        customemail.send_to_custom_email("hello"),
        action
    ]

    list_pods_1 >> list_pods_2 >> query_pod_volume >> final_call


@dag(dag_id='pod_memory_analysis')
def pod_memory_analysis():
    list_pods_1 = k8s_feedbacks.list_pods("monitoring")

    list_pods_2 = k8s_feedbacks.list_pods("prometheus")

    query_pod_volume = k8s_feedbacks.get_pod_volume("prometheus")

    action = k8s_actions.increase_pod_volume("monitoring")

    final_call = [
        slack.send_to_slack("hello"),
        pagerduty.send_to_pagerduty("hello"),
        customemail.send_to_custom_email("hello"),
        action
    ]

    list_pods_1 >> list_pods_2 >> query_pod_volume >> final_call


@dag(dag_id='node_cpu_analysis')
def node_cpu_analysis():
    list_pods_1 = k8s_feedbacks.list_pods("monitoring")

    list_pods_2 = k8s_feedbacks.list_pods("prometheus")

    action = k8s_actions.increase_pod_volume("monitoring")

    final_call = [
        slack.send_to_slack("hello"),
        pagerduty.send_to_pagerduty("hello"),
        customemail.send_to_custom_email("hello"),
        action
    ]

    list_pods_1 >> list_pods_2 >> final_call
