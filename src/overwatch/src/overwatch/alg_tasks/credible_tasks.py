#!/usr/bin/env python3
from __future__ import unicode_literals

import json
import time
from urllib.parse import urlencode
from xml.dom import minidom

import requests
from bs4 import BeautifulSoup

from . import database_tasks, report_tasks, alg_utils

CREDIBLE_USERNAME = alg_utils.get_secret('credible', 'username')
CREDIBLE_PASSWORD = alg_utils.get_secret('credible', 'password')
CREDIBLE_DOMAIN = alg_utils.get_secret('credible', 'domain')
CREDIBLE_USER_ID = alg_utils.get_secret('credible', 'emp_id')
CREDIBLE_DOMAIN_ID = alg_utils.get_secret('credible', 'domain_id')


def update_employee_email(emp_id, emp_email):
    emp_username = report_tasks.get_emp_field(emp_id, 'username')[0][0]
    emp_profile_code = report_tasks.get_emp_field(emp_id, 'profile_code')[0][0]
    cookie_jar = get_cbh_cookie()
    header = {'Content-Type': 'application/x-www-form-urlencoded'}
    add_data_url = 'https://ww7.crediblebh.com/employee/user_update.asp'
    add_data = {
        'emp_id': emp_id,
        'action': 'update',
        'username': emp_username,
        'email': emp_email,
        'profile_code': emp_profile_code
    }
    results = requests.post(
        add_data_url,
        headers=header,
        data=urlencode(add_data),
        cookies=cookie_jar)
    logout(cookie_jar)


def batch_adjust_notes(results):
    cookie_jar = get_cbh_cookie()
    session = requests.Session()
    for result_dict in results:
        if result_dict != 0:
            service_id = next(iter(result_dict.keys()))
            arbitration_package = result_dict[service_id]
            service_package = arbitration_package['service_package']
            arbitration = arbitration_package['arbitration']
            if arbitration['approve']:
                approve_service(service_package, session, cookie_jar)
            if not arbitration['approve']:
                red_x_package = arbitration['red_x_package']
                set_single_red_x(service_package, red_x_package, session, cookie_jar)
            success = report_tasks.check_service_adjustment(service_id)
            completed = 0
            if success:
                completed = 1
            database_tasks.record_arbitration_end(service_package['clientvisit_id'], time.time(), completed)
    logout(cookie_jar)


def approve_batch_services(services_package, cookie_jar):
    for service_id in services_package:
        approve_service(services_package[service_id], cookie_jar)


def approve_service(service_package, session, cookie_jar):
    service_url = 'https://ww7.crediblebh.com/visit/clientvisit_view.asp?action=approve&clientvisit_id='
    if cookie_jar is 0:
        return 0
    clientvisit_id = service_package['clientvisit_id']
    service_url += clientvisit_id
    session.get(
        service_url,
        cookies=cookie_jar
    )
    return cookie_jar


def set_batch_red_x(red_x_packages, cookie_jar):
    for service_id in red_x_packages:
        red_x_package = red_x_packages[service_id]['red_x_package']
        service_package = red_x_packages[service_id]['service_package']
        set_single_red_x(
            red_x_package=red_x_package,
            service_package=service_package,
            cookie_jar=cookie_jar
        )


def set_single_red_x(service_package, red_x_package, session, cookie_jar):
    service_url = 'https://ww7.crediblebh.com/visit/clientvisit_update.asp'
    header = {'Content-Type': 'application/x-www-form-urlencoded'}
    if cookie_jar is 0:
        return 0
    data = {
        'clientvisit_id': service_package['clientvisit_id'],
        'action': 'update',
        'dd_billmatrix': service_package['matrix_id'],
        'dd_location': service_package['location_id'],
        'recipient_id': service_package['recipient_id'],
        'manual_redx': '1',
        'manual_redx_note': str(red_x_package)
    }
    session.post(
        service_url,
        headers=header,
        data=urlencode(data),
        cookies=cookie_jar
    )
    return data


def update_admin_time_type_batch(fixable_punch_dict):
    cookie_jar = get_cbh_cookie()
    for admintime_id in fixable_punch_dict:
        punch_type = fixable_punch_dict[admintime_id]['punch_type']
        time_in = fixable_punch_dict[admintime_id]['TimeIn']
        update_admin_time_type(
            admintime_id=admintime_id,
            punch_type=punch_type,
            time_in=time_in,
            cookie_jar=cookie_jar
        )
    logout(cookie_jar)


def update_admin_time_type(admintime_id, punch_type, time_in, cookie_jar):
    header = {'Content-Type': 'application/x-www-form-urlencoded'}
    if cookie_jar is 0:
        return 0
    update_admin_time_url = 'https://ww7.crediblebh.com/employee/admintime_update.asp'
    data = {
        'admintime_id': admintime_id,
        'action': 'update',
        'admintype_id': punch_type,
        'TimeIn': time_in,
        'appr': '1'
    }
    requests.post(
        update_admin_time_url,
        headers=header,
        data=urlencode(data),
        cookies=cookie_jar
    )
    return 1


def add_employee(first_name, last_name):
    url = 'https://ww7.crediblebh.com/employee/emp_update.asp'
    cookie_jar = get_cbh_cookie()
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'action': 'add',
        'first_name': first_name,
        'last_name': last_name,
        'mobile_phone': '000-000-0000',
        'programteam': 'T-52'
    }
    requests.post(
        url,
        headers=headers,
        data=urlencode(data),
        cookies=cookie_jar
    )
    logout(cookie_jar=cookie_jar)


def update_employee_page(emp_id, field_name, value):
    url = 'https://ww7.crediblebh.com/employee/emp_update.asp'
    cookie_jar = get_cbh_cookie()
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'emp_id': emp_id,
        'action': 'update',
        field_name: value
    }
    requests.post(
        url,
        headers=headers,
        data=urlencode(data),
        cookies=cookie_jar
    )
    logout(cookie_jar)


def update_employee_page_batch(emp_id, update_data):
    url = 'https://ww7.crediblebh.com/employee/emp_update.asp'
    cookie_jar = get_cbh_cookie()
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'emp_id': emp_id
    }
    for key_value in update_data.keys():
        data[key_value] = update_data.get(key_value)
    requests.post(
        url,
        headers=headers,
        data=urlencode(data),
        cookies=cookie_jar
    )
    logout(cookie_jar)


def update_admin_time(admintime_id, date, end_time):
    header = {'Content-Type': 'application/x-www-form-urlencoded'}
    cookie_jar = get_cbh_cookie()
    if cookie_jar is 0:
        return 0
    update_admin_time_url = 'https://ww7.crediblebh.com/employee/admintime_update.asp'
    end_time = end_time.replace('AM', 'am')
    end_time = end_time.replace('PM', 'pm')
    data = {
        'admintime_id': admintime_id,
        'action': 'update',
        'rev_datein': date,
        'rev_timeout': end_time,
        'appr': '1'
    }
    requests.post(
        update_admin_time_url,
        headers=header,
        data=urlencode(data),
        cookies=cookie_jar
    )
    logout(cookie_jar)
    return 1


def add_admin_time_batch(emp_id, punches):
    header = {'Content-Type': 'application/x-www-form-urlencoded'}
    cookie_jar = get_cbh_cookie()
    if cookie_jar is 0:
        return 0
    add_admin_time_url = 'https://ww7.crediblebh.com/employee/admintime_add.asp'
    for punch in punches:
        date = punch['date']
        time_in = punch['time_in']
        time_out = punch['time_out']
        punch_type = punch['punch_type_id']
        notes = punch['notes']
        data = {
            'action': 'add',
            'emp_id': emp_id,
            'DateIn': date,
            'TimeIn': time_in,
            'TimeOut': time_out,
            'admintype_id': punch_type,
            'notes': notes
        }
        requests.post(
            add_admin_time_url,
            headers=header,
            data=urlencode(data),
            cookies=cookie_jar
        )
    logout(cookie_jar)
    return 1


def add_admin_time(emp_id, date, time_in, time_out, punch_type, notes):
    header = {'Content-Type': 'application/x-www-form-urlencoded'}
    cookie_jar = get_cbh_cookie()
    if cookie_jar is 0:
        return 0
    add_admin_time_url = 'https://ww7.crediblebh.com/employee/admintime_add.asp'
    data = {
        'action': 'add',
        'emp_id': emp_id,
        'DateIn': date,
        'TimeIn': time_in,
        'TimeOut': time_out,
        'admintype_id': punch_type,
        'notes': notes
    }
    requests.post(
        add_admin_time_url,
        headers=header,
        data=urlencode(data),
        cookies=cookie_jar
    )
    logout(cookie_jar=cookie_jar)
    return 1


def get_missed_psych_appointments(client_id, start_date, end_date):
    url = 'https://reportservices.crediblebh.com/reports/ExportService.asmx/ExportDataSet'
    key = database_tasks.get_report_key('z_alg_getMissedPsych')

    payload = {
        'connection': key,
        'start_date': '',
        'end_date': '',
        'custom_param1': client_id,
        'custom_param2': start_date,
        'custom_param3': end_date
    }
    cr = requests.get(url, params=payload)
    raw_xml = cr.content
    document = minidom.parseString(raw_xml).childNodes[0]
    elements = document.getElementsByTagName("missed_psych")[0]
    if len(elements) is 0:
        return 0
    else:
        number = elements.firstChild.data
        return number


def get_next_psych_appointment(client_id):
    url = 'https://reportservices.crediblebh.com/reports/ExportService.asmx/ExportDataSet'
    key = database_tasks.get_report_key('z_alg_getNextPsych')

    payload = {
        'connection': key,
        'start_date': '',
        'end_date': '',
        'custom_param1': client_id,
        'custom_param2': '',
        'custom_param3': ''
    }
    cr = requests.get(url, params=payload)
    raw_xml = cr.content
    document = minidom.parseString(raw_xml).childNodes[0]
    elements = document.getElementsByTagName("psych_appt")
    if len(elements) is 0:
        return 'none scheduled'
    else:
        number = elements.firstChild.data
        number = number.replace('12:00AM ', '')
        number = number.replace('Jan 1 1900 ', '')
        return number


def login_credible():
    cookie_jar = get_cbh_cookie()
    home_page = requests.get('https://crediblebh.com', cookies=cookie_jar)
    print(home_page.content)


def update_single_client_batch(client_id, updates, cookie_jar):
    if cookie_jar is 0:
        return 0
    url = 'https://ww7.crediblebh.com/client/client_update.asp'
    header = {'Content-Type': 'application/x-www-form-urlencoded'}
    params = {'p': CREDIBLE_DOMAIN_ID, 'e': CREDIBLE_USER_ID}
    starter_data = {
        'client_id': client_id,
        'action': 'update',
    }
    data = starter_data.copy()
    data.update(updates)
    requests.post(url, data=urlencode(data), cookies=cookie_jar, params=params, headers=header)
    return 1


def update_client_batch(client_data_packages):
    cookie_jar = get_cbh_cookie()
    for client_id in client_data_packages:
        change_information = {}
        for profile_field in client_data_packages[client_id]:
            value = client_data_packages[client_id][profile_field]
            change_information[profile_field] = value
        update_single_client_batch(
            client_id=client_id,
            updates=change_information,
            cookie_jar=cookie_jar
        )
    logout(cookie_jar=cookie_jar)


def update_client(client_id, field, value):
    cookie_jar = get_cbh_cookie()
    if cookie_jar is 0:
        return 0
    url = 'https://ww7.crediblebh.com/client/client_update.asp'
    header = {'Content-Type': 'application/x-www-form-urlencoded'}
    params = {'p': CREDIBLE_DOMAIN_ID, 'e': CREDIBLE_USER_ID}
    data = {
        'client_id': client_id,
        'action': 'update',
        field: value
    }
    requests.post(url, data=urlencode(data), cookies=cookie_jar, params=params, headers=header)
    logout(cookie_jar)
    return 1


# client_id_packets is a list of dict
# of the format {'client_id': client_id, 'reason': reason}
def close_client_batch(client_id_packets):
    receipt = []
    cookie_jar = get_cbh_cookie()
    for client_id_packet in client_id_packets:
        client_id = client_id_packet['client_id']
        reason = client_id_packet['reason']
        result = close_single_client_in_batch(
            client_id=client_id,
            reason=reason,
            cookie_jar=cookie_jar
        )
        receipt.append([client_id, result])
    logout(cookie_jar=cookie_jar)
    return receipt


def close_single_client_in_batch(client_id, reason, cookie_jar):
    if cookie_jar is 0:
        return 0
    url = 'https://ww7.crediblebh.com/client/client_update.asp'
    header = {'Content-Type': 'application/x-www-form-urlencoded'}
    params = {'p': CREDIBLE_DOMAIN_ID, 'e': CREDIBLE_USER_ID}
    data = {
        'client_id': client_id,
        'action': 'update',
        'client_status': 'CLOSED',
        'text24': reason
    }
    requests.post(url, data=urlencode(data), cookies=cookie_jar, params=params, headers=header)
    return 1


def close_client(client_id, reason):
    cookie_jar = get_cbh_cookie()
    if cookie_jar is 0:
        return 0
    url = 'https://ww7.crediblebh.com/client/client_update.asp'
    header = {'Content-Type': 'application/x-www-form-urlencoded'}
    params = {'p': CREDIBLE_DOMAIN_ID, 'e': CREDIBLE_USER_ID}
    data = {
        'client_id': client_id,
        'action': 'update',
        'client_status': 'CLOSED',
        'text24': reason
    }
    requests.post(url, data=urlencode(data), cookies=cookie_jar, params=params, headers=header)
    logout(cookie_jar)
    return 1


def add_insurance(client_id, payer_id, insurance_id, start_date, end_date):
    # cookie_jar = get_cbh_cookie()
    header = {'Content-Type': 'application/x-www-form-urlencoded'}
    url = 'https://ww7.crediblebh.com/client/list_ins.asp'
    params = {'p': CREDIBLE_DOMAIN_ID, 'e': CREDIBLE_USER_ID}
    data = {
        'action': 'add',
        'client_id': client_id,
        'billing_ord': 1,
        'payer_id': payer_id,
        'group_no': '',
        'ins_id': insurance_id,
        'start_date': start_date,
        'end_date': end_date,
        'copay': '',
        'credential_group': '',
        'auth_required': '',
        'is_pending': 0,
        'employeeorschool': '',
        'notes': '',
        'btn_add': 'Add Insurance',
    }
    return 'suggest adding insurance for client: ' + str(client_id) + ', payer_id: ' + str(payer_id)


#   insurance_post = requests.post(url, data=urlencode(data), cookies=cookie_jar, params=params, headers=header)


def delete_insurance(client_id, clientins_id):
    # cookie_jar = get_cbh_cookie()
    header = {'Content-Type': 'application/x-www-form-urlencoded'}
    url = 'https://ww7.crediblebh.com/client/list_ins.asp'
    params = {'p': CREDIBLE_DOMAIN_ID, 'e': CREDIBLE_USER_ID}
    data = {
        'action': 'delete',
        'client_id': client_id,
        'clientins_id': clientins_id,
        'sfilter': 1,
    }
    # insurance_post = requests.post(url, data=urlencode(data), cookies=cookie_jar, params=params, headers=header)


def deactivate_insurance(client_id, clientins_id):
    # cookie_jar = get_cbh_cookie()
    header = {'Content-Type': 'application/x-www-form-urlencoded'}
    url = 'https://ww7.crediblebh.com/client/list_ins.asp'
    params = {'p': CREDIBLE_DOMAIN_ID, 'e': CREDIBLE_USER_ID}
    data = {
        'action': 'delete',
        'client_id': client_id,
        'clientins_id': clientins_id,
        'sfilter': 1,
        'btn_inactive': 'Inactivate',
    }
    return 'suggest removing insurance listing: ' + str(clientins_id) + ' for client: ' + str(client_id)


#   insurance_post = requests.post(url, data=urlencode(data), cookies=cookie_jar, params=params, headers=header)


def get_client_insurance(client_id):
    insurances = dict()
    url = 'https://reportservices.crediblebh.com/reports/ExportService.asmx/ExportDataSet'
    key = database_tasks.get_report_key('_alg insurance_check')

    payload = {
        'connection': key,
        'start_date': '',
        'end_date': '',
        'custom_param1': client_id,
        'custom_param2': '',
        'custom_param3': ''
    }
    cr = requests.get(url, params=payload)
    raw_xml = cr.content
    document = minidom.parseString(raw_xml).childNodes[0]
    elements = document.getElementsByTagName("clientins_id")
    for element in elements:
        clientins_id = element.firstChild.data
        spacer = element.nextSibling
        payer_element = spacer.nextSibling
        payer_id = payer_element.firstChild.data
        second_spacer = payer_element.nextSibling
        payer_name_element = second_spacer.nextSibling
        payer_name = payer_name_element.firstChild.data
        insurances[payer_id] = [clientins_id, payer_name]
    return insurances


def get_cbh_cookie():
    attempts = 0
    while attempts < 3:
        try:
            jar = requests.cookies.RequestsCookieJar()
            api_url = "https://login-api.crediblebh.com/api/Authenticate/CheckLogin"
            index_url = "https://ww7.crediblebh.com/index.aspx"
            first_payload = {'UserName': CREDIBLE_USERNAME,
                             'Password': CREDIBLE_PASSWORD,
                             'DomainName': CREDIBLE_DOMAIN}
            headers = {'DomainName': CREDIBLE_DOMAIN}
            post = requests.post(api_url, json=first_payload, headers=headers)
            response_json = post.json()
            session_cookie = response_json['SessionCookie']
            jar.set('SessionId', session_cookie, domain='.crediblebh.com', path='/')
            second_payload = {'SessionId': session_cookie}
            second_post = requests.post(index_url, data=second_payload, cookies=jar)
            history = second_post.history
            cbh_response = history[0]
            cbh_cookies = cbh_response.cookies
            return cbh_cookies
        except KeyError or ConnectionError:
            attempts += 1
    return 0


def get_client_ids():
    client_ids = dict()
    url = 'https://reportservices.crediblebh.com/reports/ExportService.asmx/ExportDataSet'
    key = database_tasks.get_report_key('_alg celery clients')
    payload = {
        'connection': key,
        'start_date': '',
        'end_date': '',
        'custom_param1': '',
        'custom_param2': '',
        'custom_param3': ''
    }
    cr = requests.get(url, params=payload)
    raw_xml = cr.content
    document = minidom.parseString(raw_xml).childNodes[0]
    elements = document.getElementsByTagName("client_id")
    for element in elements:
        spacer = element.nextSibling
        status_element = spacer.nextSibling
        status = status_element.firstChild.data
        number = int(element.firstChild.data)
        client_ids[number] = status
    return client_ids


def get_medicaid_number(credible_id):
    url = 'https://reportservices.crediblebh.com/reports/ExportService.asmx/ExportDataSet'
    key = 'fDFeukXL8A6v2xAzAQR2S2w2SQh9w9k0bo6GtZyZ8FCmtX0P7bwOfAn6pV39HONE'

    payload = {
        'connection': key,
        'start_date': '',
        'end_date': '',
        'custom_param1': credible_id,
        'custom_param2': '',
        'custom_param3': ''
    }
    cr = requests.get(url, params=payload)
    raw_xml = cr.content
    document = minidom.parseString(raw_xml).childNodes[0]
    elements = document.getElementsByTagName("medicaidid")[0]
    number = elements.firstChild.data
    return number


def logout(cookie_jar):
    logout_url = 'https://ww7.crediblebh.com/secure/logout.aspx'
    requests.get(
        logout_url,
        cookies=cookie_jar
    )


def update_report_keys():
    all_updated = True
    get_web_users_url = 'https://reports.crediblebh.com/services/exports_services.asmx/GetAssignedExportsUsers'
    assign_web_user = 'https://reports.crediblebh.com/services/exports_services.asmx/AssignExportUser'
    cookie_jar = get_cbh_cookie()
    ws_json_request = requests.get(get_web_users_url, cookies=cookie_jar)
    ws_json = ws_json_request.text
    entries = json.loads(ws_json)
    sql_data = report_tasks.get_report_as_dict(686, ['', '', ''])
    for entry in entries['data']:
        export_user_id = entry['exportwsuser_id']
        report_id = entry['exportbuilder_id']
        if export_user_id is '':
            params = {
                'exportwsuser_id': '1',
                'exportbuilder_id': report_id
            }
            requests.post(assign_web_user, cookies=cookie_jar, data=params)
            all_updated = False
        else:
            sql = sql_data[report_id]['custom_query']
            sql = sql.replace('\n', ' ')
            report_data = {
                'report_id': report_id,
                'report_name': entry['export_name'],
                'report_sql': sql,
                'report_key': entry['connection']
            }
            database_tasks.add_report(
                report_data=report_data
            )
    logout(cookie_jar)
    if all_updated is True:
        return 1
    else:
        return 0


def get_ws_reports():
    returned_data = []
    first_key = get_key(686)
    all_keys_data = get_first_report(first_key)
    exportbuilder_ids = all_keys_data.getElementsByTagName('exportbuilder_id')
    for element in exportbuilder_ids:
        id_number = element.firstChild.data
        spacer = element.nextSibling
        report_name_element = spacer.nextSibling
        report_name = report_name_element.firstChild.data
        second_spacer = report_name_element.nextSibling
        report_sql_element = second_spacer.nextSibling
        report_sql = report_sql_element.firstChild.data
        new_key = get_key(id_number)
        key_data = {
            'report_id': id_number,
            'report_name': report_name,
            'report_sql': report_sql,
            'report_key': new_key
        }
        returned_data.append(key_data)
        print(key_data)
    return returned_data


def get_key(key_id):
    params = {'exportbuilder_id': str(key_id)}
    cookie_jar = get_cbh_cookie()
    cookie_jar.set('Domain', 'your_domain_name', domain='.crediblebh.com', path='/')
    url = 'https://reports.crediblebh.com/reports/web_services_export.aspx'
    key_get = requests.get(url, cookies=cookie_jar, params=params)
    nsc_cookie = key_get.cookies
    requests.cookies.merge_cookies(cookie_jar, nsc_cookie)
    key_get_2 = requests.get(url, cookies=cookie_jar, params=params)
    content = key_get_2.content
    document = BeautifulSoup(content, "html.parser")
    table_data = document.find('td')
    return table_data.contents[12]


def get_first_report(first_key):
    url = 'https://reportservices.crediblebh.com/reports/ExportService.asmx/ExportDataSet'
    key = first_key

    payload = {
        'connection': key,
        'start_date': '',
        'end_date': '',
        'custom_param1': '',
        'custom_param2': '',
        'custom_param3': ''
    }
    cr = requests.get(url, params=payload)
    raw_xml = cr.content
    document = minidom.parseString(raw_xml).childNodes[0]
    return document
