# https://www.restack.io/docs/airflow-knowledge-apache-webhook-connect-rest-api-providers-http-pypi

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from notifiers import slack, pagerduty, customemail, discord
from actions import k8s_actions
from feedbacks import k8s_feedbacks

from datetime import datetime
import logging
logger = logging.getLogger(__name__)

default_args = {
    'start_date': datetime(2021, 1, 1)
}

with DAG('pod_volume_analysis', default_args=default_args, schedule_interval=None) as dag:

    list_pods_1 = k8s_feedbacks.list_pods("monitoring")

    list_pods_2 = k8s_feedbacks.list_pods("prometheus")

    query_pod_volume = k8s_feedbacks.get_pod_volume("prometheus")

    action = k8s_actions.increase_pod_volume("monitoring")

    notify = (lambda **kwargs: [
            slack.send_to_slack(kwargs),
            pagerduty.send_to_pagerduty(kwargs),
            customemail.send_to_custom_email(kwargs)
        ])(),

    list_pods_1 >> list_pods_2 >> query_pod_volume >> [notify, action]


with DAG('pod_memory_analysis', default_args=default_args, schedule_interval=None) as dag:

    list_pods_1 = k8s_feedbacks.list_pods("monitoring")

    list_pods_2 = k8s_feedbacks.list_pods("prometheus")

    query_pod_volume = k8s_feedbacks.get_pod_volume("prometheus")

    action = k8s_actions.increase_pod_volume("monitoring")

    notify = (lambda **kwargs: [
            slack.send_to_slack(kwargs),
            pagerduty.send_to_pagerduty(kwargs),
            customemail.send_to_custom_email(kwargs)
        ])(),

    list_pods_1 >> list_pods_2 >> query_pod_volume >> [notify, action]



with DAG('node_cpu_analysis', default_args=default_args, schedule_interval=None) as dag:

    list_pods_1 = k8s_feedbacks.list_pods("monitoring")

    list_pods_2 = k8s_feedbacks.list_pods("prometheus")

    action = k8s_actions.increase_pod_volume("monitoring")

    notify = (lambda **kwargs: [
            slack.send_to_slack(kwargs),
            pagerduty.send_to_pagerduty(kwargs),
            customemail.send_to_custom_email(kwargs)
        ])(),

    list_pods_1 >> list_pods_2 >> [notify, action]
