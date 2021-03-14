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


def _get_step_record(
        step_index_name: str, expression: str, expression_values: dict,
        results: list, idx: int, step_database: db.Dynamo):
    """Queries Dynamo for all of the steps matching the given query expression and adds the results to the given
    results list

    :param step_index_name the name of the index to use with the query expression
    :param expression a query expression string to query Dynamo with
    :param expression_values a dictionary to map parameters in the expression string to actual values with
    :param results a list to add the query results to
    :param idx the index of the results list to add the Dynamo results to
    :param step_database the Dynamo database object to use for querying the data
    """
    records = step_database.get_records_with_index(step_index_name, expression, expression_values)
    results[idx] = records[0]


def _include_step_record(record: dict) -> bool:
    """Filters a given step record dictionary based on step status

    :param record a step record containing that step's status on EMR
    :returns True to include the step and False to exclude based on the check
    """
    return record["status"].upper() in {"PENDING", "CANCEL_PENDING", "RUNNING"}


def _get_steps_in_range(date_range: list, step_index_name: str, step_database: db.Dynamo) -> list:
    """Retrieves all the EMR step records from Dynamo in the given date range

    :param date_range a list of date string objects to query Dynamo for
    :param step_index_name the name of the index to use for querying the step data
    :param step_database a Dynamo DB object to submit the queries with
    :returns a list of record lists where each sublist is the result of a discrete step execution date
    """
    threads = [Thread()] * len(date_range)
    results = [None] * len(date_range)
    for idx, step_submitted_date in enumerate(date_range):
        logging.info("Retrieving steps for date {0}".format(step_submitted_date))
        expression = "{0} = :val".format("submitted_date")
        expression_values = {':val': step_submitted_date}
        threads[idx] = Thread(
                target=_get_step_record,
                args=(step_index_name, expression, expression_values, results, idx, step_database,)
            )
        threads[idx].start()
    for thread in threads:
        thread.join()
    return list(filter(lambda x: len(x) > 0, results))


def _get_step_info_from_aws(cluster_id: str, step_id: str) -> dict:
    """Retrieves the current information from EMR for a given step_id and cluster_id combination

    :param cluster_id the ID of the cluster where the step was submitted to
    :param step_id the ID of the EMR step
    :returns a dictionary containing the API response from EMR with data about the current state of the step
    """
    return emr.get_step_status(cluster_id, step_id)


def _update_step(step_record: dict, steps_database: db.Dynamo) -> None:
    """Updates the Dynamo record of the given step with the latest information from EMR

    :param step_record a dictionary containing the step's data as present in Dynamo
    :param steps_database the database to submit the updated record to
    """
    aws_step_info = _get_step_info_from_aws(step_record["cluster_id"], step_record["job_id"])
    step_record["status"] = aws_step_info["State"]
    step_record["creation_datetime"] = date.to_string(aws_step_info["Timeline"]["CreationDateTime"])
    step_record["start_datetime"] = date.to_string(aws_step_info["Timeline"].get("StartDateTime", ""))
    step_record["end_datetime"] = date.to_string(aws_step_info["Timeline"].get("EndDateTime", ""))
    steps_database.insert_records([step_record])


def _update_steps(step_records: list, steps_database: db.Dynamo) -> None:
    """Updates all of the Dynamo step records in the given records list with their latest statuses from EMR

    :param step_records a list of Dynamo records for individual EMR steps
    :param steps_database the database to submit the updated records to
    """
    for record in step_records:
        if _include_step_record(record):
            _update_step(record, steps_database)


def _update_step_records_in_dynamo(steps_database: db.Dynamo, step_records: list) -> None:
    """Updates all of the Dynamo step records in the given records list with their latest statuses from EMR
    within separate threads

    :param steps_database the database to submit the updated records to
    :param step_records a list of Dynamo records for individual EMR steps
    """
    threads = [Thread()] * len(step_records)
    for idx, records in enumerate(step_records):
        threads[idx] = Thread(target=_update_steps, args=(records, steps_database,))
        threads[idx].start()
    for thread in threads:
        thread.join()


def step_poller(event, context):
    logger.setup_logger()
    env = os.environ

    clusters_db = env["sparkflow_clusters_db"]
    clusters_index_name = env["sparkflow_clusters_index_name"]
    steps_db = env["sparkflow_step_db"]
    steps_index_name = env["sparkflow_steps_index_name"]
    polling_date_range = int(env["polling_date_range"])
    # Get database objects to store to and retrieve data from
    cluster_database = db.get_db("DYNAMO")()
    cluster_database.connect(clusters_db)
    steps_database = db.get_db("DYNAMO")()
    steps_database.connect(steps_db)

    # Get all steps in Dynamo
    date_range = _get_time_range_for_polling(polling_date_range)
    step_records = _get_steps_in_range(date_range, steps_index_name, steps_database)

    # Update their states from latest status in EMR
    _update_step_records_in_dynamo(steps_database, step_records)

    return {}
