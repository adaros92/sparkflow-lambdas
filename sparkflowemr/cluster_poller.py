import logging
import os

from sparkflowemr.utils import logger


def cluster_poller(event, context):
    logger.setup_logger()
    env = os.environ

    return {}
