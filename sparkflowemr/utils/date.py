from datetime import datetime, timedelta


def get_today() -> datetime:
    """Returns today as a datetime object"""
    return datetime.now()


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
    if not datetime_object:
        return datetime_object
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


def get_date_range(lookback_days: int, from_date: datetime = get_today()) -> list:
    """Returns a list of datetime objects in the range provided by reference date and lookback days from that
    date
    :param lookback_days the number of days to look back from the from_date to create the range
    :param from_date the end date to look back from in the range
    :returns a list of dates in the range
    """
    return [from_date - timedelta(days=x) for x in range(lookback_days)]


def get_string_date_range(
        lookback_days: int, from_date: datetime = get_today(), formatting: str = "%Y-%m-%d") -> list:
    """Returns a list of string datetimes in the range provided by reference date and lookback days from that
    date
    :param lookback_days the number of days to look back from the from_date to create the range
    :param from_date the end date to look back from in the range
    :param formatting the formatting to assume for the dates in the range
    :returns a list of string dates in the range
    """
    date_range = get_date_range(lookback_days, from_date + timedelta(days=2))
    return [datetime_object.strftime(formatting) for datetime_object in date_range]
