#!/usr/bin/env python3
import collections
import time
from datetime import datetime
from xml.dom import minidom

import requests

from . import alg_utils
from . import database_tasks


def get_csw_teams():
    csw_teams = {}
    team_data = get_report_as_dict(649, [])
    for team_id in team_data:
        if team_data[team_id]['external_id'] == '7' and 'ACT' not in team_data[team_id]['team_name']:
            csw_teams[team_id] = team_data[team_id]
    return csw_teams


def get_credible_email_report():
    return get_report_as_dict(755, [])


def get_credible_username_check():
    return get_report_as_dict(756, [])


def get_clinical_command(team_name):
    team_report = get_report_as_dict(757, [team_name])
    return team_report


def get_audit_targets(start_date, end_date, num_targets):
    targets = {}
    selected_targets = {}
    credible_start_date = alg_utils.py_date_to_credible_date(start_date)
    credible_end_date = alg_utils.py_date_to_credible_date(end_date)
    target_data = get_report_as_dict(751, [credible_start_date, credible_end_date])
    for service_id in target_data:
        team_name = target_data[service_id]['team_name']
        if team_name not in targets:
            targets[team_name] = {}
        targets[team_name][service_id] = target_data[service_id]
    for team_name in targets:
        possible_targets = targets[team_name]
        unique_clients = alg_utils.select_unique_values(possible_targets, 'client_id')
        if len(unique_clients) >= num_targets:
            team_targets = []
            targeted_clients = []
            count = 0
            while count < num_targets:
                possible_target = alg_utils.select_random_service(possible_targets)
                possible_client_id = possible_target['client_id']
                while possible_client_id in targeted_clients:
                    possible_target = alg_utils.select_random_service(possible_targets)
                    possible_client_id = possible_target['client_id']
                targeted_clients.append(possible_client_id)
                team_targets.append(possible_target['clientvisit_id'])
                count += 1
        else:
            team_targets = []
        selected_targets[team_name] = team_targets
    return selected_targets


def check_tx_plan_date(service_id):
    plan_dict = get_report_as_dict(752, [service_id])
    tx_indicator = int(plan_dict[service_id]['has_valid_plan'])
    if tx_indicator is 1:
        return True
    if tx_indicator is 0:
        return False


def check_service_adjustment(service_id):
    service_dict = get_report_as_dict(750, [service_id])
    keys = service_dict.keys()
    key = next(iter(keys))
    if key is 0:
        return False
    elif key is 1:
        return True


def get_given_field(field_id, service_id):
    return get_report_as_dict(747, [field_id, service_id])


def get_service_package(service_id):
    service_dict = get_report_as_dict(742, [service_id])
    keys = service_dict.keys()
    key = next(iter(keys))
    return service_dict[key]


def check_service_for_clones(presentation, response, clientvisit_id):
    return get_report_as_dict(739, [presentation, response, clientvisit_id])


def get_commsupt_fields(service_id):
    return get_report_as_dict(738, [service_id])[service_id]


def get_unapproved_pilot_whales():
    unapproved_ids = []
    whale_dict = get_report_as_dict(744, [])
    for service_id in whale_dict:
        unapproved_ids.append(service_id)
    return unapproved_ids


def get_unapproved_commsupt():
    unapproved_ids = []
    whale_dict = get_report_as_dict(731, [])
    for service_id in whale_dict:
        unapproved_ids.append(service_id)
    return unapproved_ids


def check_null_punch(null_punch_dict):
    emp_id = null_punch_dict['emp_id']
    iso_date = null_punch_dict['rev_timeout']
    partner_punch_id = get_report_as_dict(737, [emp_id, iso_date])
    if len(partner_punch_id) > 0:
        return True
    else:
        return False


def get_null_punches():
    returned_report = {}
    null_punches = get_report_as_dict(736, [])
    for service_id in null_punches:
        null_punch = null_punches[service_id]
        emp_id = null_punch['emp_id']
        rev_timeout = null_punch['rev_timeout']
        time_in = null_punch['timein']
        time_in = alg_utils.credible_backend_datetime_to_credible_frontend_datetime(time_in)
        day_out = alg_utils.credible_datetime_to_iso_date(rev_timeout)
        punch_dict = {
            'emp_id': emp_id,
            'admintime_id': service_id,
            'rev_timeout': day_out,
            'TimeIn': time_in,
            'punch_type': '20'
        }
        if emp_id not in returned_report:
            returned_report[emp_id] = {
                service_id: punch_dict
            }
        else:
            returned_report[emp_id][service_id] = punch_dict
    return returned_report


def get_profile_changes():
    cleaned_report = {}
    returned_report = {}
    report = get_report_as_dict(726, [])
    for client_id in report.keys():
        client_line = []
        update_fields = report[client_id]
        for field in update_fields:
            value = update_fields[field]
            if value is 'x':
                value = ''
            client_line.append([field, value])
        client_line.pop(0)
        cleaned_report[client_id] = client_line
    for client_id in cleaned_report:
        returned_line = collections.OrderedDict()
        client_data_dict = cleaned_report[client_id]
        for data_dict in client_data_dict:
            if 'change' in data_dict[0]:
                indicator = data_dict[1]
                if indicator is '1':
                    pointer = client_data_dict.index(data_dict)
                    data_field = client_data_dict[pointer + 1]
                    returned_line[data_field[0]] = data_field[1]
        returned_report[client_id] = returned_line
    return returned_report


def get_benefits():
    report = get_report_as_dict(694, ['', '', ''])
    return report


def get_admin_time_id_end(emp_id, date_in, time_in):
    py_date = datetime.strptime(date_in, '%m/%d/%y').date()
    py_time = datetime.strptime(time_in, '%I:%M %p').time()
    rev_timein = datetime.combine(py_date, py_time)
    params = [emp_id, rev_timein.strftime('%m/%d/%Y %H:%M:%S %p'), '']
    admin_id_fields = get_google_formatted_report(701, params=params)
    admin_id_fields.pop(0)
    if len(admin_id_fields) > 0:
        end_time = admin_id_fields[0][1]
        py_end_time = datetime.strptime(end_time, '%H:%M:%S:%f').time()
        sql_end_time = py_end_time.strftime('%I:%M %p')
        returned_package = {
            'end_time': sql_end_time,
            'admintime_id': int(admin_id_fields[0][0])
        }
        return returned_package
    else:
        return 0


def get_admin_time_id(emp_id, date_in, time_in):
    py_date = datetime.strptime(date_in, '%m/%d/%y').date()
    py_time = datetime.strptime(time_in, '%I:%M %p').time()
    rev_timein = datetime.combine(py_date, py_time)
    params = [emp_id, rev_timein.strftime('%m/%d/%Y %H:%M:%S %p'), '']
    admin_id_fields = get_google_formatted_report(701, params=params)
    admin_id_fields.pop(0)
    if len(admin_id_fields) > 0:
        return int(admin_id_fields[0][0])
    else:
        return 0


def get_hire_date(emp_id):
    params = [emp_id, 'hire_date', '']
    field_report = get_dynamic_google_formatted_report(693, params=params)
    field_report.pop(0)
    credible_date = field_report[0]
    py_date = time.strptime(credible_date[0][:19], '%Y-%m-%dT%H:%M:%S')
    return_date = time.strftime('%m/%d/%Y', py_date)
    return return_date


def get_emp_field(emp_id, field_name):
    field_report = get_dynamic_google_formatted_report(693, [emp_id, field_name, ''])
    field_report.pop(0)
    return field_report


def get_paychex_employees():
    emp_report = get_google_formatted_report(692, [])
    emp_report.pop(0)
    return emp_report


def get_dynamic_google_formatted_report(report_id, params):
    returned_report = []
    header = []
    document = get_report_by_id(
        report_id=report_id,
        params=params,
    )
    header_row_element = document.getElementsByTagName(
        'xs:sequence'
    ).item(1)
    header_rows = header_row_element.getElementsByTagName(
        'xs:element'
    )
    for header_row in header_rows:
        header.append(header_row.getAttribute('name'))
    returned_report.append(header)
    data_elements = document.getElementsByTagName('Table1')
    for data_element in data_elements:
        new_line = []
        for field in header:
            data_cell = data_element.getElementsByTagName(field).item(0)
            new_line.append(data_cell.firstChild.nodeValue)
        returned_report.append(new_line)
    return returned_report


def get_google_formatted_report(report_id, params):
    try:
        returned_report = []
        document = get_report_by_id(
            report_id=report_id,
            params=params,
        )
        if document is 0:
            return returned_report
        else:
            tables = parse_static_tables(document)
            returned_report.extend(tables)
            return returned_report
    except AttributeError:
        print('report id ' + str(report_id) + ' with parameters ' + str(params) + ' returned no values ')
        return []


def get_report_as_dict(report_id, params):
    dict_report = collections.OrderedDict()
    report = get_google_formatted_report(
        report_id=report_id,
        params=params
    )
    try:
        header = report.pop(0)
    except IndexError:
        return dict_report
    for row in report:
        pk_guess = row[0]
        count = 0
        report_line = collections.OrderedDict()
        for field in row:
            report_line[header[count]] = field
            count += 1
        dict_report[pk_guess] = report_line
    return dict_report


def get_credible_data(sql):
    returned_tables = []
    report_id = 712
    url = 'https://reportservices.crediblebh.com/reports/ExportService.asmx/ExportDataSet'
    key = database_tasks.get_report_key_by_id(report_id)
    payload = {
        'connection': key,
        'start_date': '',
        'end_date': '',
        'custom_param1': sql,
        'custom_param2': '',
        'custom_param3': ''
    }
    cr = requests.get(url, params=payload)
    raw_xml = cr.content
    document = minidom.parseString(raw_xml)
    returned_tables.append(parse_header(document))
    returned_tables.extend(parse_tables(document))
    return returned_tables


def parse_static_tables(document):
    header = parse_static_header(document)
    table = [header]
    data_sets = document.getElementsByTagName('NewDataSet')
    if len(data_sets) is 0:
        return []
    else:
        data_sets = data_sets[0]
        for data_set in data_sets.childNodes:
            if data_set.nodeType == data_set.ELEMENT_NODE:
                if data_set.localName == 'Table':
                    row = []
                    row_dict = {}
                    for entry in data_set.childNodes:
                        if entry.nodeType == entry.ELEMENT_NODE:
                            data = entry.firstChild.data
                            header_field = entry.nodeName
                            if len(data) > 45000:
                                data = data[:45000]
                            row_dict[header_field] = data
                    for header_field in header:
                        try:
                            data = row_dict[header_field]
                        except KeyError:
                            data = ''
                        row.append(data)
                    table.append(row)
        return table


def parse_static_header(document):
    header = []
    header_row_element = document.getElementsByTagName(
        'xs:sequence'
    ).item(0)
    header_rows = header_row_element.getElementsByTagName(
        'xs:element'
    )
    for header_row in header_rows:
        header.append(header_row.getAttribute('name'))
    return header


def parse_tables(document):
    table = []
    data_sets = document.getElementsByTagName('NewDataSet')[0]
    for data_set in data_sets.childNodes:
        if data_set.nodeType == data_set.ELEMENT_NODE:
            if data_set.localName != 'Table':
                row = []
                for entry in data_set.childNodes:
                    if entry.nodeType == entry.ELEMENT_NODE:
                        data = entry.firstChild.data
                        if len(data) > 45000:
                            data = data[:45000]
                        row.append(data)
                table.append(row)
    return table


def parse_header(document):
    header = []
    header_row_element = document.getElementsByTagName(
        'xs:sequence'
    ).item(1)
    header_rows = header_row_element.getElementsByTagName(
        'xs:element'
    )
    for header_row in header_rows:
        header.append(header_row.getAttribute('name'))
    return header


def get_report_by_id(report_id, params):
    completed = False
    tries = 0
    if len(params) is 0:
        params = ['', '', '']
    if len(params) is 1:
        params.append('')
        params.append('')
    if len(params) is 2:
        params.append('')
    url = 'https://reportservices.crediblebh.com/reports/ExportService.asmx/ExportDataSet'
    key = database_tasks.get_report_key_by_id(report_id)

    payload = {
        'connection': key,
        'start_date': '',
        'end_date': '',
        'custom_param1': params[0],
        'custom_param2': params[1],
        'custom_param3': params[2]
    }
    while not completed and tries < 3:
        cr = requests.get(url, params=payload)
        raw_xml = cr.content
        document = minidom.parseString(raw_xml).childNodes[0]
        if len(document.childNodes) > 0:
            return document
        else:
            tries += 1
    print('report with report id ' + str(report_id) + ' and params ' + str(params) + ' could not be fetched')
    return 0


def get_emp_email(emp_id):
    email_data = get_report('Z_alg_emails', [emp_id])
    elements = email_data.getElementsByTagName("email")
    if len(elements) is 0:
        return ''
    else:
        email = elements[0].firstChild.data
        return email


def get_report(report_name, params):
    if len(params) is 0:
        params = ['', '', '']
    if len(params) is 1:
        params.append('')
        params.append('')
    if len(params) is 2:
        params.append('')
    url = 'https://reportservices.crediblebh.com/reports/ExportService.asmx/ExportDataSet'
    key = database_tasks.get_report_key(report_name)

    payload = {
        'connection': key,
        'start_date': '',
        'end_date': '',
        'custom_param1': params[0],
        'custom_param2': params[1],
        'custom_param3': params[2]
    }
    cr = requests.get(url, params=payload)
    raw_xml = cr.content
    document = minidom.parseString(raw_xml).childNodes[0]
    return document
