#!/usr/bin/env python3
import collections

import pymysql
import pymysql.cursors
import pymysql.err

from . import alg_utils

db_address = alg_utils.get_secret('db', 'address')
db_name = alg_utils.get_config('db', 'name')
trivium_name = alg_utils.get_config('db', 'trivium_name')
pump_name = alg_utils.get_config('db', 'pump_name')
db_un = alg_utils.get_secret('db', 'username')
db_pw = alg_utils.get_secret('db', 'password')


def store_similarity(local_service_id, foreign_service_id, field_id, similarity):
    db = get_autocommit_db()
    cursor = db.cursor()
    cursor.callproc('addSimilarity', [
        local_service_id,
        foreign_service_id,
        field_id,
        similarity
    ])
    cursor.close()
    db.close()


def check_single_service_status(service_id):
    results = get_procedure_as_dict('checkArbitration', [service_id])
    if len(results) > 0:
        return False
    else:
        return True


def check_arbitration_status(service_ids):
    unarbitrated_ids = []
    service_ids_sting = ''
    for service_id in service_ids:
        service_ids_sting += str(service_id) + ', '
    service_ids_sting = service_ids_sting[0:(len(service_ids_sting) - 2)]
    sql = 'SELECT ServiceArbitration.service_id FROM ServiceArbitration ' \
          'WHERE ServiceArbitration.service_id IN (' + service_ids_sting + ') AND ' \
                                                                           'ServiceArbitration.completed = 1 AND ' \
                                                                           'ServiceArbitration.end_time IS NOT ' \
                                                                           'NULL; '
    db = get_simple_db()
    cursor = db.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()
    for result in results:
        unarbitrated_ids.append(result[0])
    cursor.close()
    db.close()
    return unarbitrated_ids


def record_arbitration_end(service_id, completed, py_timestamp):
    db = get_autocommit_db()
    cursor = db.cursor()
    end_time = alg_utils.py_timestamp_to_mysql_timestamp(py_timestamp)
    cursor.callproc('finishArbitration', [service_id, end_time, completed])
    cursor.close()
    db.close()


def record_arbitration_start(service_id, py_timestamp):
    db = get_autocommit_db()
    cursor = db.cursor()
    start_time = alg_utils.py_timestamp_to_mysql_timestamp(py_timestamp)
    cursor.callproc('startArbitration', [service_id, start_time])
    cursor.close()
    db.close()


def get_procedure_as_dict(procedure_name, params):
    returned_report = {}
    db = get_simple_db()
    cursor = db.cursor()
    cursor.callproc(procedure_name, params)
    header = cursor.description
    rows = cursor.fetchall()
    for row in rows:
        returned_row = {}
        counter = 0
        pk_guess = row[0]
        for field in row:
            returned_row[header[counter][0]] = field
            counter += 1
        returned_report[pk_guess] = returned_row
    cursor.close()
    db.close()
    return returned_report


def update_punch_admin_id(punch_id, admin_id):
    db = get_trivium_db()
    cursor = db.cursor()
    args = [punch_id, admin_id]
    cursor.callproc('addPunchAdminId', args)
    cursor.close()
    db.close()


def get_pending_punches():
    punches = collections.OrderedDict()
    db = get_trivium_db()
    cursor = db.cursor()
    cursor.callproc('getPendingPunches', [])
    rows = cursor.fetchall()
    for row in rows:
        emp_id = row[1]
        employee_id_keys = punches.keys()
        new_punch = [
            alg_utils.json_serial(row[2]),
            alg_utils.json_serial(alg_utils.timedelta_to_time(row[3])),
            row[4],
            row[5],
            row[0]
        ]
        if emp_id in employee_id_keys:
            emp_record = punches.get(emp_id)
            emp_record.append(new_punch)
        else:
            emp_record = [new_punch]
            punches[emp_id] = emp_record
    return punches


def add_trivium_punch(credible_id, punch_date, punch_time, hours, punch_type_id):
    done = False
    attempts = 0
    while done is False and attempts < 5:
        try:
            db = get_trivium_db()
            cursor = db.cursor()
            args = [
                credible_id,
                punch_date,
                punch_time,
                hours,
                punch_type_id
            ]
            cursor.callproc('addPunch', args=args)
            done = True
            cursor.close()
            db.close()
        except pymysql.err.OperationalError:
            attempts += 1
        except pymysql.IntegrityError:
            print('could not find an employee record for user ' + str(credible_id))
            return


def get_credible_id(paychex_id):
    db = get_trivium_db()
    cursor = db.cursor()
    cursor.callproc('getCredibleId', [paychex_id])
    row = cursor.fetchone()
    credible_id = row[0]
    cursor.close()
    db.close()
    return credible_id


def get_type_map():
    type_map = dict()
    db = get_trivium_db()
    cursor = db.cursor()
    cursor.callproc('getTypeMap', [])
    rows = cursor.fetchall()
    for row in rows:
        type_id = row[0]
        type_description = row[3]
        type_map[type_description] = type_id
    return type_map


def add_trivium_employee(credible_id, paychex_id, last_name, first_name):
    db = get_trivium_db()
    cursor = db.cursor()
    args = [
        credible_id,
        paychex_id,
        last_name,
        first_name
    ]
    cursor.callproc('addEmployee', args)
    cursor.close()
    db.close()


def get_current_trivium_roster():
    roster = {}
    db = get_trivium_db()
    cursor = db.cursor()
    cursor.callproc('getEmployees')
    rows = cursor.fetchall()
    for row in rows:
        employee = {
            'credible_id': row[0],
            'paychex_id': row[1],
            'last_name': row[2],
            'first_name': row[3]
        }
        roster[row[0]] = employee
    return roster


def get_report_common_name(report_id):
    db = get_simple_db()
    cursor = db.cursor()
    cursor.callproc('getCommonName', [report_id])
    row = cursor.fetchone()
    return str(row[0])


def get_mailing_list():
    mail_list = {}
    db = get_simple_db()
    cursor = db.cursor()
    cursor.callproc('getMailingLists')
    rows = cursor.fetchall()
    for row in rows:
        emp_id = row[0]
        key_id = row[1]
        param_1 = row[2]
        param_2 = row[3]
        param_3 = row[4]
        mail_args = {
            'key_id': key_id,
            'param1': param_1,
            'param2': param_2,
            'param3': param_3
        }
        if emp_id not in mail_list.keys():
            mail_list[emp_id] = [mail_args]
        else:
            current_mail_args = mail_list.get(emp_id)
            current_mail_args.append(mail_args)
    return mail_list


def get_report_key(report_name):
    db = get_simple_db()
    cursor = db.cursor()
    cursor.callproc('getReportKey', [report_name])
    results = cursor.fetchone()
    return results[0]


def get_report_key_by_id(report_id):
    db = get_simple_db()
    cursor = db.cursor()
    cursor.callproc('getReportKeyById', [report_id])
    results = cursor.fetchone()
    return results[0]


def add_report(report_data):
    db = get_autocommit_db()
    cursor = db.cursor()
    args = [
        report_data['report_id'],
        report_data['report_name'],
        report_data['report_sql'],
        report_data['report_key'],
    ]
    cursor.callproc('addReport', args)
    cursor.close()
    db.close()


def get_simple_eligibility(client_id):
    db = get_simple_db()
    cursor = db.cursor()
    cursor.callproc('getSimplifiedEligibility', [client_id])
    returned_data = cursor.fetchone()
    print(returned_data)
    args = {
        'client_id': returned_data[0],
        'scrub_id': returned_data[2],
        'is_medicaid': returned_data[4],
        'is_mco': returned_data[3],
        'is_medicare': returned_data[5],
        'is_tpl': returned_data[6],
        'is_ltc': returned_data[7],
        'is_uninsured': returned_data[8],
        'description': returned_data[9]
    }
    cursor.close()
    db.close()
    return args


def add_scrub_stub(client_id, start_time):
    db = get_autocommit_db()
    cursor = db.cursor()
    cursor.callproc('addScrubStub', [client_id, start_time])
    returned_args = cursor.fetchone()
    cursor.close()
    db.close()
    return returned_args[0]


def add_eligibility(eligibility_args):
    proc = 'addSimpleEligibility'
    db = get_server_cursor_db()
    cursor = db.cursor()
    cursor.callproc(proc, eligibility_args)
    db.commit()
    cursor.close()
    db.close()


def add_client(client_id, client_status):
    db = get_autocommit_db()
    cursor = db.cursor()
    args = [client_id, client_status]
    cursor.callproc('addClient', args)
    cursor.close()
    db.close()


def update_client_status(client_id, client_status):
    db = get_autocommit_db()
    cursor = db.cursor()
    args = [client_id, client_status]
    cursor.callproc('updateEligibilityStatus', args)
    cursor.close()
    db.close()


def get_current_roster():
    roster = set()
    db = get_simple_db()
    cursor = db.cursor()
    cursor.callproc('getCurrentClients')
    rows = cursor.fetchall()
    cursor.close()
    db.close()
    return rows


def get_scrub_id(credible_id):
    db = get_server_cursor_db()
    args = [credible_id]
    cursor = db.cursor()
    cursor.callproc('getScrubId', args)
    result = cursor.fetchone()
    scrub_id = 0
    if result:
        scrub_id = result[0]
    cursor.close()
    db.close()
    return scrub_id


def store_suggestions(client_id, suggestions):
    db = get_autocommit_db()
    cursor = db.cursor()
    args = [client_id, suggestions]
    cursor.callproc('addSuggestion', args)
    cursor.close()
    db.close()


def store_html(scrub_id, client_id, end_time, html):
    db = get_autocommit_db()
    cursor = db.cursor()
    cursor.callproc('addHtml', [scrub_id, client_id, end_time, html])
    cursor.close()
    db.close()


def get_scrubbed_html():
    data = dict()
    db = get_simple_db()
    cursor = db.cursor()
    scrubbed_html = cursor.execute("getUninspectedScrubs();")
    results = cursor.fetchall()
    for row in results:
        credible_id = row[0]
        scrub_id = row[1]
        html_text = row[2]
        entry = {'credible_id': credible_id, 'scrub_id': scrub_id, 'html': html_text}
        data[credible_id] = entry
    cursor.close()
    db.close()
    return data


def get_scrubbed_html_row_by_row():
    db = get_server_cursor_db()
    cursor = db.cursor()
    return cursor


def get_autocommit_db():
    db = pymysql.connect(
        db_address,
        user=db_un,
        passwd=db_pw,
        db=db_name)
    db.autocommit(True)
    return db


def get_pump_db():
    db = pymysql.connect(
        db_address,
        user=db_un,
        passwd=db_pw,
        db=pump_name)
    db.autocommit(True)
    return db


def get_trivium_db():
    db = pymysql.connect(
        db_address,
        user=db_un,
        passwd=db_pw,
        db=trivium_name)
    db.autocommit(True)
    return db


def get_simple_db():
    db = pymysql.connect(
        db_address,
        user=db_un,
        passwd=db_pw,
        db=db_name)
    return db


def get_server_cursor_db():
    db = pymysql.connect(
        db_address,
        user=db_un,
        passwd=db_pw,
        db=db_name,
        cursorclass=pymysql.cursors.SSCursor)
    return db
