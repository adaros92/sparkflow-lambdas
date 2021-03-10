import logging
import os

from sparkflowemr.utils import logger, validation
from sparkflowtools.models import db


def _parse_event_inputs(event: dict) -> dict:
    """Parses event input provided to Lambda

    :param event the event dictionary passed in as Lambda input
    :returns the parsed event input to use downstream
    """
    required_input = ["step_config", "pool_id"]
    validation.validate_event_inputs(event, required_input, {})
    # TODO any parsing needed?
    return event


def _get_all_clusters_under_pool(pool_id: str, clusters_db: db.Dynamo, index_name: str) -> list:
    """Gets all clusters under the given pool_id from Dynamo to find one that will accept a new step

    :param pool_id the ID of the cluster pool to retrieve clusters from
    :param clusters_db the database to use for retrieving the clusters from
    :param index_name the name of the index to use when querying the table containing the clusters
    :returns a list of cluster dictionaries from DynamoDB
    """
    try:
        expression = "cluster_pool_id = :val"
        expression_values = {':val': pool_id}
        return clusters_db.get_records_with_index(index_name, expression, expression_values)[0]
    except Exception as e:
        logging.warning("could not retrieve clusters under pool_id {0} from {1} using {2}".format(
            pool_id, clusters_db, index_name))
        logging.exception(e)
        raise


def step_manager(event, context):
    logger.setup_logger()
    env = os.environ
    parsed_event = _parse_event_inputs(event)
    logging.info("parsed the following from the event input - {0}".format(parsed_event))
    # Get the config parameters from the Lambda environment
    clusters_db = env["sparkflow_clusters_db"]
    clusters_index_name = env["sparkflow_clusters_index_name"]
    steps_db = env["sparkflow_job_runs"]
    step_config = parsed_event["step_config"]
    pool_id = parsed_event["pool_id"]
    # Get database objects to store to and retrieve data from
    cluster_database = db.get_db("DYNAMO")()
    cluster_database.connect(clusters_db)
    steps_database = db.get_db("DYNAMO")()
    steps_database.connect(steps_db)

    clusters = _get_all_clusters_under_pool(pool_id, cluster_database, clusters_index_name)

    return {}
