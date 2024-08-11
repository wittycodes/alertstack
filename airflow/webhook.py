from datetime import datetime

from airflow.plugins_manager import AirflowPlugin
from airflow.utils.state import State
from airflow.www.app import csrf
from flask import Flask, request, jsonify, Blueprint
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging
logger = logging.getLogger(__name__)

webhooks_api_blueprint = Blueprint(
    'webhooks_api_blueprint',
    __name__,
    url_prefix='/webhooks'
)


def trigger_dag(dag_id, conf):
    # Trigger the specified DAG
    # Replace this with your logic to trigger the DAG
    # For example, using the Airflow API or creating a DAG run manually
    # ...
    dag = DAG(dag_id, start_date=days_ago(1))  # Replace with your DAG's details
    dag_run = dag.create_dagrun(
        run_id=f"webhook_triggered_{datetime.now()}",
        state=State.RUNNING,
        conf=conf,
        external_trigger=True
    )
    return dag_run


@webhooks_api_blueprint.route('/prometheus', methods=['POST'])
@csrf.exempt
def prometheus_webhook():
    data = request.get_json()
    # alert_name = data['alerts'][0]['labels']['alertname']  # Extract alert name
    # dag_id = f"dag_{alert_name}"  # Create DAG ID based on alert name

    # Replace with your logic to validate alert and determine target DAG
    # ...
    logger.info(data)
    logger.info("before webhook_prometheus_entrypoint")
    trigger_dag("webhook_prometheus_entrypoint", conf=data)
    return jsonify({'status': 'success'})


class REST_API_Plugin(AirflowPlugin):
    """Creating the REST_API_Plugin which extends the AirflowPlugin so its imported into Airflow"""
    name = "webhooks"
    operators = []
    flask_blueprints = [webhooks_api_blueprint]
    hooks = []
    executors = []
    menu_links = []
