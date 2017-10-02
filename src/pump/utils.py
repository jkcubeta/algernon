import json
from datetime import datetime


def get_config(config_category, config_name):
    with open('/algernon', 'r') as secret_file:
        json_string = secret_file.read()
        config_dict = json.loads(json_string)
        return config_dict[config_category][config_name]


def get_secret(secret_category, secret_name):
    try:
        with open('/run/secrets/alg_secrets', 'r') as secret_file:
            json_string = secret_file.read()
            try:
                secret_dict = json.loads(json_string)
                return secret_dict[secret_category][secret_name]
            except ValueError:
                return json_string
    except IOError:
        return None


def credible_datetime_to_py_date(credible_datetime):
    py_datetime = credible_datetime_to_py_datetime(credible_datetime)
    return py_datetime.date()


def credible_datetime_to_py_datetime(credible_datetime):
    credible_datetime = credible_datetime.replace('-04:00', '')
    py_datetime = datetime.strptime(credible_datetime, '%Y-%m-%dT%H:%M:%S')
    return py_datetime
