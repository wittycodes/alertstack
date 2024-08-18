from datetime import datetime

import notifiers.thirdparty.slack, notifiers.thirdparty.pagerduty, notifiers.thirdparty.customemail, notifiers.thirdparty.discord
import enrichment.aws.vm
import remediation.aws.vm
from airflow.decorators import task, dag

default_args = {
    'start_date': datetime(2021, 1, 1)
}

@dag(dag_id='node_cpu_analysis', default_args=default_args, schedule_interval=None)
def node_cpu_analysis():
    cpu_util = enrichment.aws.vm.get_cpu()

    action = remediation.aws.vm.modify_vm_type()

    final_call = [
        notifiers.thirdparty.slack.send_to_slack(),
        notifiers.thirdparty.pagerduty.send_to_pagerduty(),
        notifiers.thirdparty.customemail.send_to_custom_email(),
        action
    ]

    cpu_util >> final_call



node_cpu_analysis()