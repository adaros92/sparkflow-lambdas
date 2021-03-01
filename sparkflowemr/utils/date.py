from datetime import datetime


def get_current_time() -> datetime:
    """Provides the current timestamp

    :returns a datetime object representing the current timestamp
    """
    return datetime.now()


def to_string(datetime_object: datetime, formatting: str = '%Y-%m-%dT%H:%M'):
    """Converts a given datetime object into a string with the optional formatting provided

    :param datetime_object the object to convert to a string
    :param formatting the formatting of the resulting string
    :returns a string representation of the datetime object following the given formatting
    """
    return datetime_object.strftime(formatting)


def get_current_time_str(formatting: str = '%Y-%m-%dT%H:%M') -> str:
    """Provides the current time as a string

    :param formatting an optional formatting to follow
    :returns the current time as a string following the given formatting
    """
    return to_string(get_current_time(), formatting)


def get_current_date_str(formatting: str = '%Y-%m-%d') -> str:
    """Provides the current date as a string

    :param formatting an optional formatting to follow
    :returns the current date as a string following the given formatting
    """
    return to_string(get_current_time(), formatting)
