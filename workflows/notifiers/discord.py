import requests


def send_to_discord(content):
    webhook_url = 'https://discord.com/api/webhooks/1271545437198880798/JfNeqOIgIhLK9dssn7TzpzN9TWoA25Jx4Rce-1-jrWRi-iX6cvdwLfv7rff0mqrvbzRG'  # Replace with your webhook URL

    data = {'content': "hu ha content"}
    response = requests.post(webhook_url, json=data)
