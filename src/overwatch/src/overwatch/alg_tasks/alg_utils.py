import json
import random
from datetime import date, datetime, time, timedelta

import requests


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


def get_audit_dates():
    today = date.today()
    last_monday = today + timedelta(days=-today.weekday())
    next_sunday = last_monday + timedelta(days=6)
    return {'start_date': last_monday, 'end_date': next_sunday}


def select_unique_values(target_dict, target_field):
    unique_values = set()
    for key in target_dict:
        try:
            target_value = target_dict[key][target_field]
            unique_values.add(target_value)
        except KeyError:
            print('specified field not in top level of dictionary')
            return unique_values
    return unique_values


def select_random_service(possible_targets):
    target_id = random.choice(list(possible_targets.keys()))
    client_id = possible_targets[target_id]['client_id']
    return {'clientvisit_id': target_id, "client_id": client_id}


def py_date_to_credible_date(py_date):
    credible_date = py_date.strftime('%m/%d/%Y')
    return credible_date


def humanize_red_x_package(red_x_package):
    human_readable = ''
    for red_x_reason in red_x_package:
        if red_x_reason is 'hotwords':
            human_readable += ' identified clinical hotwords: '
            for field in red_x_package[red_x_reason]:
                human_readable += ' in ' + field + '('
                for hotword in red_x_package[red_x_reason][field]:
                    human_readable += hotword + ', '
                human_readable = human_readable[0:len(human_readable) - 2]
                human_readable += ')'
            human_readable += '\n'
        elif red_x_reason is 'clones':
            human_readable += ' note contains cloned elements in the following fields: '
            for match_type in red_x_package[red_x_reason]:
                human_readable += match_type + ' ('
                clone_ids = red_x_package[red_x_reason][match_type]
                if len(clone_ids) > 3:
                    human_readable += str(clone_ids[0:3]) + ' + ' + str(len(clone_ids) - 3) + ' others), '
                else:
                    for clone_id in clone_ids:
                        human_readable += (str(clone_id) + ', ')
                    human_readable = human_readable[0:len(human_readable) - 2]
                human_readable += ')'
            human_readable = human_readable[0:len(human_readable) - 2]
            human_readable += '\n'
        elif red_x_reason is 'ghosts':
            human_readable += ' potential ghost note: '
            for ghost_trigger in red_x_package[red_x_reason]:
                if ghost_trigger is 'character_results':
                    human_readable += ' very few characters in '
                    for field_name in red_x_package[red_x_reason][ghost_trigger]:
                        human_readable += (field_name + ', ')
                    human_readable = human_readable[0:len(human_readable) - 2]
                elif ghost_trigger is 'word_results':
                    human_readable += ' very few words in '
                    for field_name in red_x_package[red_x_reason][ghost_trigger]:
                        human_readable += (field_name + ', ')
                    human_readable = human_readable[0:len(human_readable) - 2]
                elif ghost_trigger is 'spelling':
                    human_readable += ' lots of non-standard words in '
                    for field_name in red_x_package[red_x_reason][ghost_trigger]:
                        human_readable += (field_name + ', ')
                    human_readable = human_readable[0:len(human_readable) - 2]
            human_readable += '\n'
        elif red_x_reason is 'tx_plan':
            if 'expired' in red_x_package[red_x_reason].keys():
                human_readable += ' tx plan is expired '
            else:
                human_readable += ' something off with tx plan '
            human_readable += '\n'
        else:
            human_readable += str(red_x_reason)
    return human_readable.rstrip()


def requests_cookie_jar_to_json(cookie_jar):
    cookie = cookie_jar._cookies['.crediblebh.com']['/']['cbh']
    tidy_cookie = {
        'domain': cookie.domain,
        'name': cookie.name,
        'path': cookie.path,
        'value': cookie.value,
        'version': cookie.version
    }
    return json.dumps(tidy_cookie)


def json_to_requests_cookie_jar(voorhees_cookies):
    cookie_dict = json.loads(voorhees_cookies)
    jar = requests.cookies.RequestsCookieJar()
    jar.set(
        domain='MBI',
        name=cookie_dict['name'],
        path=cookie_dict['path'],
        value=cookie_dict['value'],
        version=cookie_dict['version']
    )
    return jar


def credible_backend_datetime_to_credible_frontend_datetime(backend_datetime):
    py_datetime = credible_datetime_to_py_date(backend_datetime)
    return py_datetime_to_credible_human_datetime(py_datetime)


def py_timestamp_to_mysql_timestamp(py_timestamp):
    mysql_timestamp = datetime.fromtimestamp(py_timestamp).strftime('%Y-%m-%d %H:%M:%S')
    return mysql_timestamp


def py_datetime_to_credible_human_datetime(py_datetime):
    return py_datetime.strftime('%m/%d/%y %I:%M:%S %p')


def credible_datetime_to_iso_date(credible_datetime):
    py_date = credible_datetime_to_py_date(credible_datetime)
    return py_date.strftime('%Y%m%d')


def credible_datetime_to_py_date(credible_datetime):
    py_datetime = credible_datetime_to_py_datetime(credible_datetime)
    return py_datetime.date()


def credible_datetime_to_py_datetime(credible_datetime):
    credible_datetime = credible_datetime.replace('-04:00', '')
    py_datetime = datetime.strptime(credible_datetime, '%Y-%m-%dT%H:%M:%S')
    return py_datetime


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date, time)):
        serial = obj.isoformat()
        return serial
    raise TypeError("Type %s not serializable" % type(obj))


def is_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


def timedelta_to_time(target):
    return (datetime.min + target).time()


def json_time_to_credible_time(json_time):
    py_time = datetime.strptime(json_time, '%H:%M:%S').time()
    credible_time = py_time.strftime('%I:%M %p')
    return credible_time


def json_date_to_credible_date(json_date):
    py_date = datetime.strptime(json_date, '%Y-%m-%d')
    credible_date = py_date.strftime('%m/%d/%y')
    return credible_date


def calculate_missing_punch(time_string, hours):
    py_time = datetime.strptime(time_string, '%H:%M:%S')
    advanced_date = py_time + timedelta(hours=hours)
    advanced_time = advanced_date.time()
    credible_time = advanced_time.strftime('%I:%M %p')
    return credible_time
