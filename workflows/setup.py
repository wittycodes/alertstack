import airflow
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Replace with actual notification functions
def send_slack_notification(ti):
    # Logic to send Slack notification
    pass

def send_email_notification(ti):
    # Logic to send email notification
    pass

def send_rocketchat_notification(ti):
    # Logic to send RocketChat notification
    pass

def send_sms_notification(ti):
    # Logic to send SMS notification
    pass

with airflow.DAG(
    'my_dag',
    start_date=days_ago(2),
    schedule_interval=None,
) as dag:

    # Your DAG tasks here

    notify = PythonOperator(
        task_id='notify',
        python_callable=lambda ti: [
            send_slack_notification(ti),
            send_email_notification(ti),
            send_rocketchat_notification(ti),
            send_sms_notification(ti)
        ],
        dag=dag
    )
