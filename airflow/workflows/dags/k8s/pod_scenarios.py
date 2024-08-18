import logging
import enrichment.k8s.pod
import remediation.k8s.pod
import notifiers.thirdparty.slack, notifiers.thirdparty.pagerduty, notifiers.thirdparty.customemail, notifiers.thirdparty.discord
from datetime import datetime

from airflow.decorators import task, dag


default_args = {
    'start_date': datetime(2021, 1, 1)
}

@dag(dag_id='pod_volume_analysis', default_args=default_args, schedule_interval=None)
def pod_volume_analysis():
    list_pods_1 = enrichment.k8s.pod.list_pods()

    list_pods_2 = enrichment.k8s.pod.list_pods()

    action = remediation.k8s.pod.increase_pod_volume()

    final_call = [
        notifiers.thirdparty.slack.send_to_slack(),
        notifiers.thirdparty.pagerduty.send_to_pagerduty(),
        notifiers.thirdparty.customemail.send_to_custom_email(),
        action
    ]

    list_pods_1 >> list_pods_2 >> final_call


@dag(dag_id='pod_memory_analysis', default_args=default_args, schedule_interval=None)
def pod_memory_analysis():
    list_pods_1 = enrichment.k8s.pod.list_pods()

    list_pods_2 = enrichment.k8s.pod.list_pods()

    action = remediation.k8s.pod.increase_pod_volume()

    final_call = [
        notifiers.thirdparty.slack.send_to_slack(),
        notifiers.thirdparty.pagerduty.send_to_pagerduty(),
        notifiers.thirdparty.customemail.send_to_custom_email(),
        action
    ]

    list_pods_1 >> list_pods_2 >> final_call



pod_memory_analysis()
pod_volume_analysis()
