import logging
logger = logging.getLogger(__name__)

from airflow.decorators import task

@task
def get_cpu(ti):
    pass