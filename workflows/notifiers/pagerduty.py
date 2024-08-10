from airflow.decorators import task


@task
def send_to_pagerduty(ti):
    # Logic to send pagerduty notification
    pass