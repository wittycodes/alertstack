import logging
logger = logging.getLogger(__name__)

from airflow.decorators import task


@task
def modify_vm_type(ti):
    pass
