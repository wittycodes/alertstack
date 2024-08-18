import requests
from airflow.decorators import task


@task
def send_to_discord(ti):
    content = ti.xcom_pull(key='notification', value=ti)
    webhook_url = 'https://discord.com/api/webhooks/1271545437198880798/JfNeqOIgIhLK9dssn7TzpzN9TWoA25Jx4Rce-1-jrWRi-iX6cvdwLfv7rff0mqrvbzRG'  # Replace with your webhook URL

    data = {'content': "Critical alert - pod-234141ncjern4oc under disk pressure " + content}
    response = requests.post(webhook_url, json=data, verify=False)
