import logging
import os

from sparkflowtools.models import db
from sparkflowtools.utils import emr
from threading import Thread

from utils import logger, date


def _get_time_range_for_polling(date_range: int) -> list:
    """Retrieves a list of discrete date strings to narrow down step records to based on submission date

    :param date_range the number of lookback days from the current date to construct the date range from
    :returns the list of date string objects
    """
    return date.get_string_date_range(date_range)


def _get_running_clusters() -> list:
    """Retrieves a list of cluster objects from AWS for clusters that are still alive

    :returns a list of cluster dictionaries containing information about those clusters from EMR
    """
    running_states = ["STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING", "TERMINATING"]
    clusters = emr.get_cluster_statuses(running_states)
    return clusters


def _distribute_records_among_threads(records: list, threads: int) -> list:
    """Creates a round robbing distribution of the given records list by the number of given threads

    :param records a list of records to distribute
    :param threads the number of threads to distribute by
    :returns a list of lists with length threads where each sub list contains a portion of the records to process
        in a separate thread
    """
    result = [[] for _ in range(threads)]
    for i in range(len(records)):
        result_bin = i % threads
        result[result_bin].append(records[i])
    return list(filter(lambda x: len(x) > 0, result))


def _update_dynamo_record(cluster_data: dict, cluster_db: db.Dynamo) -> None:
    """Updates a single record in Dynamo based on the given cluster_data dictionary

    :param cluster_data a dictionary containing information about a cluster to update
    :param cluster_db the database object to run the update with
    """
    cluster_id_to_update = cluster_data["cluster_id"]
    cluster_record = {}
    try:
        cluster_record = cluster_db.get_record({"cluster_id": cluster_id_to_update})[0]
    except Exception as e:
        logging.exception(e)
        pass
    if cluster_record:
        cluster_record["number_of_steps"] = cluster_data["number_of_active_steps"]
        cluster_record["state"] = cluster_data["status"]
        cluster_record["creation_datetime"] = cluster_data["creation_datetime"]
        cluster_record["end_datetime"] = cluster_data["end_datetime"]
        cluster_record["state_change_reason"] = cluster_data["state_change_reason"]
        cluster_record["instance_hours"] = cluster_data["instance_hours"]
        cluster_db.insert_records([cluster_record])


def _update_dynamo_records(clusters_to_update: list, cluster_db: db.Dynamo) -> None:
    """Updates multiple clusters in dynamo

    :param clusters_to_update a list of cluster dictionaries containing the data for each cluster to update
    :param cluster_db the database object to run the update with
    """
    for cluster_data in clusters_to_update:
        _update_dynamo_record(cluster_data, cluster_db)


def _update_dynamo_records_in_threads(clusters_to_update: list, cluster_db: db.Dynamo, threads: int = 4) -> None:
    """Updates a collection of EMR clusters in separate threads

    :param clusters_to_update: a list of lists containing cluster data to update
    :param cluster_db the database object to run the update with
    :param threads: the number of threads to perform the update in
    """
    distributed_clusters_to_update = _distribute_records_among_threads(clusters_to_update, threads)
    threads = [Thread()] * len(distributed_clusters_to_update)
    for idx, records in enumerate(distributed_clusters_to_update):
        threads[idx] = Thread(target=_update_dynamo_records, args=(records, cluster_db,))
        threads[idx].start()
    for thread in threads:
        thread.join()


def cluster_poller(event, context):
    logger.setup_logger()
    env = os.environ

    clusters_db = env["sparkflow_clusters_db"]
    clusters_index_name = env["sparkflow_clusters_index_name"]
    polling_date_range = int(env["polling_date_range"])
    # Get database objects to store to and retrieve data from
    cluster_database = db.get_db("DYNAMO")()
    cluster_database.connect(clusters_db)

    # Get all clusters from AWS
    date_range = _get_time_range_for_polling(polling_date_range)

    # Get all clusters currently running
    clusters = _get_running_clusters()

    # Update Dynamo with latest cluster information
    _update_dynamo_records_in_threads(clusters, cluster_database)

    return {}
