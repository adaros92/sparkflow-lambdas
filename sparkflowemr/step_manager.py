import logging
import os

from sparkflowemr.utils import logger


def step_manager(event, context):
    logger.setup_logger()
    env = os.environ

    return {}
