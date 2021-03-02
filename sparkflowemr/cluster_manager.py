import logging
import os

from utils import logger, date
from sparkflowtools.models import db, cluster


def _validate_event_inputs(event: dict) -> None:
    """Validates that the input provided to the Lambda is correct

    :param event the event dictionary passed in as Lambda input
    """
    required_inputs = ["emr_config", "operation"]
    for required_input in required_inputs:
        if required_input not in event:
            raise ValueError("{0} must be part of the event input passed in to cluster_manager".format(required_input))
    valid_operations = {"create", "delete"}
    operation = event["operation"]
    if operation not in valid_operations:
        raise ValueError("operation must be one of {0} but found {1}".format(valid_operations, operation))


def _parse_event_inputs(event: dict) -> dict:
    """Parses event input provided to Lambda

    :param event the event dictionary passed in as Lambda input
    :returns the parsed event input to use downstream
    """
    _validate_event_inputs(event)
    # TODO any parsing needed?
    return event


def _get_pool_id(clusters: list) -> str:
    """Generates a pool ID from a list of individual cluster objects

    :param clusters a list of clusters to generate a pool ID from
    :returns a generated pool ID string
    """
    import random
    import string
    assert len(clusters) > 0
    return "pool-" + clusters[-1].cluster_id + ''.join(random.choice(string.ascii_letters) for _ in range(5))


def _create_pool_of_clusters(emr_configs: list, cluster_builder: cluster.EmrBuilder) -> tuple:
    """Creates a pool of clusters from a given list of EMR configs

    :param emr_configs a list of EMR config dictionaries that represent individual clusters to be created
    :param cluster_builder the builder object to use for creating the clusters
    :returns a tuple consisting of a list of clusters created an a unique pool_id the clusters belong to
    """
    cluster_records = []
    logging.info("Creating {0} EMR clusters".format(len(emr_configs)))
    for config in emr_configs:
        cluster_launched = cluster_builder.build_from_config(config)
        cluster_records.append(cluster_launched)
    return cluster_records, _get_pool_id(cluster_records)


def _delete_pool_of_clusters(pool_id: str, index_name: str, clusters_db: db.Dynamo):
    expression = "cluster_pool_id = :val"
    expression_values = {':val': {'S': pool_id}}
    records = clusters_db.get_records_with_index(index_name, expression, expression_values)
    for record in records:
        cluster_name = record["name"]
        cluster_id = record["cluster_id"]
        cluster_object = cluster.EmrCluster(cluster_name).cluster_id = cluster_id
        logging.info("Terminating cluster {0} in pool {1}".format(cluster_id, pool_id))
        cluster_object.terminate()


def _create_record_from_cluster(cluster_object: cluster.EmrCluster):
    return {
        "cluster_id": cluster_object.cluster_id,
        "name": cluster_object.name,
        "state": cluster_object.state,
        "fleet-type": cluster_object.fleet_type,
        "tags": cluster_object.tags
    }


def _pesist_created_clusters(
        clusters: list, pool_id: str, clusters_db: db.Dynamo, cluster_pool_db: db.Dynamo) -> None:
    """Records individual cluster objects and the associated pool_id in DynamoDB

    :param clusters a list of EMR cluster objects to record
    :param pool_id the unique ID of the pool of clusters to record
    :param clusters_db the DynamoDB object to record the individual clusters with
    :param cluster_pool_db the DynamoDB object to record the cluster pool ID with
    """
    update_date = date.get_current_date_str()
    records = []
    logging.info("Recording {0} clusters in {1}".format(len(clusters), clusters_db.table_name))
    for cluster_object in clusters:
        dynamo_record = _create_record_from_cluster(cluster_object)
        dynamo_record["update_date"] = update_date
        dynamo_record["cluster_pool_id"] = pool_id
        records.append(dynamo_record)
    try:
        clusters_db.insert_records(records)
    except Exception as e:
        logging.warning("Could not insert cluster records {0}".format(records))
        # TODO cleanup by removing all clusters launched when one record can't be inserted
        logging.exception(e)
    cluster_pool_record = [{"cluster_pool_id": pool_id, "update_date": update_date}]
    try:
        logging.info("Recording cluster pool ID {0} in {1}".format(pool_id, cluster_pool_db.table_name))
        cluster_pool_db.insert_records(cluster_pool_record)
    except Exception as e:
        logging.warning("Could not insert cluster pool record {0}".format(cluster_pool_record))
        # TODO cleanup by removing all clusters launched when one record can't be inserted
        logging.exception(e)


def cluster_manager(event: dict, context: dict) -> dict:
    # Set up logger and retrieve the Lambda environment containing config
    logger.setup_logger()
    env = os.environ
    # Parse and validate input
    parsed_event = _parse_event_inputs(event)
    logging.info("parsed the following from the event input - {0}".format(parsed_event))
    # Get the config parameters from the Lambda environment
    cluster_pool_db = env["sparkflow_cluster_pool_db"]
    clusters_db = env["sparkflow_clusters_db"]
    clusters_index_name = env["sparkflow_clusters_index_name"]
    operation = parsed_event["operation"]
    # Get database objects to store to and retrieve data from
    cluster_database = db.get_db("DYNAMO")()
    cluster_database.connect(clusters_db)
    cluster_pool_database = db.get_db("DYNAMO")()
    cluster_pool_database.connect(cluster_pool_db)

    if operation == "create":
        # Create a new cluster pool from a list of provided configs
        cluster_builder = cluster.EmrBuilder()
        cluster_records, pool_id = _create_pool_of_clusters(parsed_event["emr_config"], cluster_builder)
        _pesist_created_clusters(cluster_records, pool_id, cluster_database, cluster_pool_database)
    elif operation == "delete":
        pool_id = parsed_event["pool_id"]
        _delete_pool_of_clusters(pool_id, clusters_index_name, cluster_database)

    return {"Status": 200}
