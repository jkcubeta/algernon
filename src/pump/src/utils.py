from datetime import datetime


def credible_datetime_to_py_date(credible_datetime):
    py_datetime = credible_datetime_to_py_datetime(credible_datetime)
    return py_datetime.date()


def credible_datetime_to_py_datetime(credible_datetime):
    credible_datetime = credible_datetime.replace('-04:00', '')
    py_datetime = datetime.strptime(credible_datetime, '%Y-%m-%dT%H:%M:%S')
    return py_datetime
