# import logging
# logger = logging.getLogger(__name__)
#
#
#
# from kubernetes import client, config
# config.load_incluster_config()
#
# v1 = client.CoreV1Api()
# apps_v1 = client.AppsV1Api()
#
#
# # List pods
# pods = v1.list_pod_for_all_namespaces(watch=False)
# for i in pods.items:
#     print(i.metadata.name)
#     logger.info(i.metadata.name)
#     logger.info("This is an info message")
#     logger.warning("This is a warning message")
#     logger.error("This is an error message")
#
