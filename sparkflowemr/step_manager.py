import logging
import os
import sys

from utils import logger, validation, date
from sparkflowtools.models import db, cluster, step


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


def _get_eligible_clusters(cluster_records: list) -> list:
    """Gets all clusters in the given records collection with the eligible statuses that can accept steps

    :param cluster_records a list of cluster records from DynamoDB
    :returns a list of cluster records filtered to just those eligible
    """
    eligible_states = ["STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING"]

    def in_states(record: dict):
        return record["state"].upper() in eligible_states
    return list(filter(in_states, cluster_records))


def _get_cluster_id_to_accept_step(cluster_records: list) -> str:
    """Retrieves the ID of the cluster to submit the step to based on fewest number of steps in a pool of clusters

    :param cluster_records a list of cluster records from DynamoDB
    :returns the cluster_id as present in EMR
    """
    assert len(cluster_records) > 0
    min_number_of_steps = sys.maxsize
    cluster_id = ""
    for cluster_record in cluster_records:
        steps_on_cluster = cluster_record.get("number_of_steps", 0)
        if steps_on_cluster < min_number_of_steps:
            cluster_id = cluster_record["cluster_id"]
            min_number_of_steps = steps_on_cluster
    return cluster_id


def _create_step_object(step_config: dict) -> step.EmrStep:
    """Creates a step object from the config as defined in sparkflowtools.models

    :param step_config a config dictionary received from the Lambda input to create the step with
    :returns a step object created from the given config
    """
    emr_step = step.EmrStep(step_config["name"])
    emr_step.action_on_failure = "CANCEL_AND_WAIT"
    emr_step.spark_args = step_config.get("spark_args", {})
    emr_step.job_args = step_config.get("job_args", {})
    emr_step.job_class = step_config["job_class"]
    emr_step.job_jar = step_config["job_jar"]
    return emr_step


def _submit_step(step_object: step.EmrStep, cluster_id: str) -> None:
    """Submits a step on an EMR cluster by the given ID

    :param step_object a step object as defined in sparkflowtools.models containing relevant step information
    :param cluster_id the ID of the cluster on EMR to submit the step on
    """
    emr_cluster = cluster.EmrCluster("")
    emr_cluster.cluster_id = cluster_id
    emr_cluster.submit_step(step_object)


def _create_step_record(step_object: step.EmrStep) -> list:
    """Creates a record to insert into Dynamo from a given step object

    :param step_object a step object as defined in sparkflowtools.models containing relevant step information
    :return: a list with the one step record to insert
    """
    creation_date = date.get_current_date_str()
    creation_datetime = date.get_current_time_str()
    return [{
        "job_id": step_object.step_id,
        "submitted_date": creation_date,
        "submitted_datetime": creation_datetime,
        "action_on_failure": step_object.action_on_failure,
        "step_name": step_object.name,
        "cluster_id": step_object.cluster_id,
        "script_path": step_object.script_path,
        "job_jar": step_object.job_jar,
        "job_args": step_object.job_args,
        "spark_args": step_object.spark_args,
        "status": "PENDING"
    }]


def _pesist_created_step(step_object: step.EmrStep, steps_db: db.Dynamo, transform_id: str) -> None:
    """Records a step on EMR in DynamoDB to expose to the sparkflow UI

    :param step_object a step object as defined in sparkflowtools.models containing relevant step information
    :param steps_db the database object to record the step data with
    :param transform_id the ID of the transform for which the step is running
    """
    step_record = _create_step_record(step_object)
    step_record[0]["transform_id"] = transform_id
    try:
        logging.info("Recording step {0} in {1}".format(step_record, steps_db.table_name))
        steps_db.insert_records(step_record)
    except Exception as e:
        logging.warning("Could not insert cluster pool record {0}".format(step_record))
        logging.exception(e)


def step_manager(event, context):
    logger.setup_logger()
    env = os.environ
    parsed_event = _parse_event_inputs(event)
    logging.info("parsed the following from the event input - {0}".format(parsed_event))
    # Get the config parameters from the Lambda environment
    clusters_db = env["sparkflow_clusters_db"]
    clusters_index_name = env["sparkflow_clusters_index_name"]
    steps_db = env["sparkflow_step_db"]
    step_config = parsed_event["step_config"]
    pool_id = parsed_event["pool_id"]
    # Get database objects to store to and retrieve data from
    cluster_database = db.get_db("DYNAMO")()
    cluster_database.connect(clusters_db)
    steps_database = db.get_db("DYNAMO")()
    steps_database.connect(steps_db)

    # Get the cluster to submit the step on
    clusters = _get_eligible_clusters(
        _get_all_clusters_under_pool(pool_id, cluster_database, clusters_index_name))
    if len(clusters) == 0:
        raise RuntimeError("No eligible clusters found: {0}".format(clusters))
    cluster_id = _get_cluster_id_to_accept_step(clusters)

    # Create the step object from the given config passed into the Lambda
    emr_step = _create_step_object(step_config)
    # Submit the step and keep a record of it on Dynamo
    _submit_step(emr_step, cluster_id)
    _pesist_created_step(emr_step, steps_database, step_config["transform_id"])

    return {}
