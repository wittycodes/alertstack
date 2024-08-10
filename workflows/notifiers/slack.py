from airflow.decorators import task


@task
def send_to_slack(ti):
    # Logic to send Slack notification
    pass