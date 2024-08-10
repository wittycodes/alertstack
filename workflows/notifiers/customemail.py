from airflow.decorators import task


@task
def send_to_custom_email(ti):
    # Logic to send Slack notification
    pass