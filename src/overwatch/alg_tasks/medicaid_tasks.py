#!/usr/bin/env python3
from __future__ import absolute_import, unicode_literals

import requests
from bs4 import BeautifulSoup

from . import alg_utils
from . import credible_tasks

medicaid_un = alg_utils.get_secret('medicaid', 'username')
medicaid_pw = alg_utils.get_secret('medicaid', 'password')


def check_id(credible_id):
    medicaid_id = credible_tasks.get_medicaid_number(credible_id)
    session_id = get_session_id()
    login(medicaid_un, medicaid_pw, session_id)
    html = get_html(medicaid_id, session_id)
    logout(session_id)
    return html


def logout(session_id):
    url = 'https://www.dc-medicaid.com/dcwebportal/logout'
    requests.get(url, cookies=session_id)


def get_html(medicaid_id, session_string):
    url = 'https://www.dc-medicaid.com/dcwebportal/inquiry/submitEligibilityInquiry'
    params = {
        "recipientId": medicaid_id,
        "lastName": "",
        "firstName": "",
        "dateOfBirth": "",
        "ssnNumber": "",
        "serviceBeginDate": "",
        "serviceEndDate": ""
    }
    r = requests.post(url, data=params, cookies=session_string)
    content = r.content
    document = BeautifulSoup(content, "html.parser")
    tables = document.findAll("table")
    concat_tables = ""
    for table in tables:
        concat_tables = concat_tables + table.prettify()
    return concat_tables


def get_session_id():
    url = 'https://www.dc-medicaid.com/dcwebportal/home'
    r = requests.get(url)
    return r.cookies


def login(username, password, session_string):
    url = 'https://www.dc-medicaid.com/dcwebportal/j_spring_security_check'
    payload = {
        'j_username': username,
        'j_password': password,
        'x': '10',
        "y": '9'
    }
    r = requests.post(url, data=payload, cookies=session_string)

