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

    list_pods_1 = PythonOperator(
        task_id='list_pods_1',
        python_callable=k8s_feedbacks.list_pods,
        op_args=['monitoring'],
        dag=dag
    )

    list_pods_2 = PythonOperator(
        task_id='list_pods_2',
        python_callable=k8s_feedbacks.list_pods,
        op_args=['prometheus'],
        dag=dag
    )

    query_pod_volume = PythonOperator(
        task_id='query_pod_volume',
        python_callable=lambda ti: k8s_feedbacks.get_pod_volume(),
        op_args=['prometheus'],
        dag=dag
    )

    action = PythonOperator(
        task_id='action',
        python_callable=lambda ti: [
            k8s_actions.increase_pod_volume("monitoring")
        ],
        dag=dag
    )

    notify = PythonOperator(
        task_id='notify',
        python_callable=lambda ti: [
            slack.send_to_slack(ti),
            pagerduty.send_to_pagerduty(ti),
            customemail.send_to_email(ti)
        ],
        dag=dag
    )

    list_pods_1 >> list_pods_2 >> query_pod_volume >> [notify, action]


    # task = SimpleHttpOperator(
    #     task_id='post_op',
    #     http_conn_id='http_default',
    #     endpoint='api/v1/external/trigger',
    #     method='POST',
    #     data='{"dag_run_id": "my_custom_run_id"}',
    #     headers={"Content-Type": "application/json"}
    # )


with DAG('pod_memory_analysis', default_args=default_args, schedule_interval=None) as dag:

    list_pods_1 = PythonOperator(
        task_id='list_pods_1',
        python_callable=k8s_feedbacks.list_pods,
        op_args=['monitoring'],
        dag=dag
    )

    list_pods_2 = PythonOperator(
        task_id='list_pods_2',
        python_callable=k8s_feedbacks.list_pods,
        op_args=['prometheus'],
        dag=dag
    )

    query_pod_volume = PythonOperator(
        task_id='query_pod_volume',
        python_callable=lambda ti: k8s_feedbacks.get_pod_volume(),
        op_args=['prometheus'],
        dag=dag
    )

    action = PythonOperator(
        task_id='action',
        python_callable=lambda ti: [
            k8s_actions.increase_pod_volume("monitoring")
        ],
        dag=dag
    )

    notify = PythonOperator(
        task_id='notify',
        python_callable=lambda ti: [
            slack.send_to_slack(ti),
            pagerduty.send_to_pagerduty(ti),
            customemail.send_to_email(ti)
        ],
        dag=dag
    )

    list_pods_1 >> list_pods_2 >> query_pod_volume >> [notify, action]


    # task = SimpleHttpOperator(
    #     task_id='post_op',
    #     http_conn_id='http_default',
    #     endpoint='api/v1/external/trigger',
    #     method='POST',
    #     data='{"dag_run_id": "my_custom_run_id"}',
    #     headers={"Content-Type": "application/json"}
    # )


with DAG('node_cpu_analysis', default_args=default_args, schedule_interval=None) as dag:

    list_pods_1 = PythonOperator(
        task_id='list_pods_1',
        python_callable=k8s_feedbacks.list_pods,
        op_args=['monitoring'],
        dag=dag
    )

    list_pods_2 = PythonOperator(
        task_id='list_pods_2',
        python_callable=k8s_feedbacks.list_pods,
        op_args=['prometheus'],
        dag=dag
    )

    # query_pod_volume = PythonOperator(
    #     task_id='query_pod_volume',
    #     python_callable=lambda ti: k8s_feedbacks.get_pod_volume(),
    #     op_args=['prometheus'],
    #     dag=dag
    # )

    action = PythonOperator(
        task_id='action',
        python_callable=lambda ti: [
            k8s_actions.increase_pod_volume("monitoring")
        ],
        dag=dag
    )

    notify = PythonOperator(
        task_id='notify',
        python_callable=lambda ti: [
            slack.send_to_slack(ti),
            # pagerduty.send_to_pagerduty(ti),
            # customemail.send_to_custom_email(ti),
            # discord.send_to_discord(ti)
        ],
        dag=dag
    )

    list_pods_1 >> list_pods_2 >> [notify, action]


    # task = SimpleHttpOperator(
    #     task_id='post_op',
    #     http_conn_id='http_default',
    #     endpoint='api/v1/external/trigger',
    #     method='POST',
    #     data='{"dag_run_id": "my_custom_run_id"}',
    #     headers={"Content-Type": "application/json"}
    # )