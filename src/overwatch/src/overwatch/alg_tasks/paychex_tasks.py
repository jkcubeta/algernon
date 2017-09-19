#!/usr/bin/env python3
import collections
import csv
import datetime
import logging
from logging import handlers

from . import alg_utils
from . import credible_tasks, database_tasks, report_tasks

MAX_SIZE = 300 * 1024 * 1024
logger = logging.getLogger('alg_scrub')
fh = logging.handlers.RotatingFileHandler(filename='paychex_tasks.log', maxBytes=MAX_SIZE, backupCount=5)
fh.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.WARNING)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)
logger.setLevel(logging.DEBUG)


def get_fixable_null_punches():
    fixable_punches = {}
    all_null_punches = report_tasks.get_null_punches()
    for emp_id in all_null_punches:
        if emp_id not in fixable_punches:
            fixable_punches[emp_id] = {}
        null_punch_dicts = all_null_punches[emp_id]
        for admintime_id in null_punch_dicts:
            null_punch_dict = null_punch_dicts[admintime_id]
            null_punch_check = report_tasks.check_null_punch(null_punch_dict)
            if null_punch_check is True:
                fixable_punches[emp_id][admintime_id] = null_punch_dict
            else:
                print(null_punch_dict)
    return fixable_punches


def update_employee_profiles():
    logger.debug('started updating employee benefits information')
    benefits = report_tasks.get_benefits()
    benefits.pop(0)
    total = len(benefits)
    progress = 1
    logger.debug('there are ' + str(total) + ' employees to be updated')
    for line in benefits:
        emp_id = line['emp_id']
        logger.debug('starting update for employee id ' + str(emp_id))
        vacation = line['vacation_time']
        sick = line['sick_time']
        personal = line['personal_time']
        bereavement = line['bereavement_time']
        credible_tasks.update_employee_page(
            emp_id=emp_id,
            field_name='text20',
            value=vacation
        )
        credible_tasks.update_employee_page(
            emp_id=emp_id,
            field_name='text21',
            value=sick
        )
        credible_tasks.update_employee_page(
            emp_id=emp_id,
            field_name='text22',
            value=personal
        )
        credible_tasks.update_employee_page(
            emp_id=emp_id,
            field_name='text23',
            value=bereavement
        )
        logger.debug('completed updating for employee id ' + emp_id)
        logger.debug('completed ' + str(progress) + '/' + str(total))
        progress += 1
    logger.debug('completed updating employee benefits')


def migrate_all_punches_to_credible():
    pending_punches = database_tasks.get_pending_punches()
    total_count = len(pending_punches)
    total_progress = 0
    for emp_id in pending_punches:
        migrate_single_employee_punches(
            emp_id=emp_id,
            pending_punches=pending_punches)
        progress = round(float(total_progress) / float(total_count) * 100, 2)
        print('\r' + str(progress) + '%', end='')
        total_progress += 1


def migrate_single_employee_punches(emp_id, pending_punches):
    checked_punches = []
    day_punches = extract_punches_by_day(pending_punches)
    punches = parse_punches(day_punches)
    for punch in punches:
        date_in = punch['date']
        time_in = punch['time_in']
        admintime_package = report_tasks.get_admin_time_id_end(
            emp_id=emp_id,
            date_in=date_in,
            time_in=time_in
        )
        if admintime_package is 0:
            checked_punches.append(punch)
        else:
            admintime_id = admintime_package['admintime_id']
            credible_end_time = admintime_package['end_time']
            if credible_end_time != punch['time_out']:
                # the original version of this program did not correctly
                # account for lunches, so some admin time entries needed
                # to be updated with the correct end_time
                update_status = credible_tasks.update_admin_time(
                    admintime_id=admintime_id,
                    date=date_in,
                    end_time=punch['time_out']
                )
                if update_status is 0:
                    logger.warning('update not completed on punch for ' + emp_id + ' on ' + date_in)
            else:
                parent_punch_ids = punch['parent_punch_ids']
                for parent_punch_id in parent_punch_ids:
                    database_tasks.update_punch_admin_id(
                        punch_id=parent_punch_id,
                        admin_id=admintime_id)
    credible_tasks.add_admin_time_batch(
        emp_id=emp_id,
        punches=checked_punches)


def parse_punches(emp_day_pending_punches):
    type_map = {
        1: 20,
        2: 23,
        3: 21,
        4: 33,
        5: 24
    }
    admin_time_entries = []
    for punch_date in emp_day_pending_punches.keys():
        day_punches = emp_day_pending_punches.get(punch_date)
        punch_times = list(day_punches.keys())
        for punch_time in punch_times:
            pointer = punch_times.index(punch_time)
            punch = day_punches.get(punch_time)
            punch_type_id = punch['punch_type_id']
            if punch_type_id in [1, 9] and len(punch_times) > pointer + 1:
                next_punch_time = punch_times[pointer+1]
                next_punch = day_punches.get(next_punch_time)
                if next_punch['punch_type_id'] in [7, 8]:
                    admin_time_entry = {
                        'date': alg_utils.json_date_to_credible_date(punch_date),
                        'time_in': alg_utils.json_time_to_credible_time(punch_time),
                        'time_out': alg_utils.json_time_to_credible_time(next_punch_time),
                        'punch_type_id': 20,
                        'notes': '',
                        'parent_punch_ids': [punch['punch_id'], next_punch['punch_id']]
                    }
                    admin_time_entries.append(admin_time_entry)
            if punch_type_id in (2, 3, 4, 5):
                hours = punch['hours']
                time_out = alg_utils.calculate_missing_punch(punch_time, hours)
                admin_time_entry = {
                    'date': alg_utils.json_date_to_credible_date(punch_date),
                    'time_in': alg_utils.json_time_to_credible_time(punch_time),
                    'time_out': time_out,
                    'punch_type_id': type_map[punch_type_id],
                    'notes': '',
                    'parent_punch_ids': [punch['punch_id']]
                }
                admin_time_entries.append(admin_time_entry)
    return admin_time_entries


def extract_punches_by_day(emp_pending_punches):
    matched_punch_records = {}
    for punch in emp_pending_punches:
        punch_date = punch[0]
        punch_time = punch[1]
        punch_type_id = punch[2]
        punch_id = punch[4]
        hours = punch[3]
        new_punch_record = {
            'punch_date': punch_date,
            'punch_time': punch_time,
            'hours': hours,
            'punch_type_id': punch_type_id,
            'punch_id': punch_id
        }
        if punch_date in matched_punch_records.keys():
            day_record = matched_punch_records.get(punch_date)
            day_record[punch_time] = new_punch_record
        else:
            day_record = collections.OrderedDict()
            day_record[punch_time] = new_punch_record
            matched_punch_records[punch_date] = day_record
    return matched_punch_records


def create_punches(spreadsheet_name):
    logger.debug('started parsing punches from spreadsheet: '+spreadsheet_name)
    skip = 0
    type_map = database_tasks.get_type_map()
    no_id = set()
    with open(spreadsheet_name, 'r', encoding='UTF-8') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        row_count = sum(1 for _ in reader)
        logger.debug('there are  ' + str(row_count) + ' punches in ' + spreadsheet_name)
        csvfile.seek(0)
        for row in reader:
            if skip is not 0:
                punch_date = row[0]
                py_date = datetime.datetime.strptime(punch_date, "%m/%d/%Y").date()
                punch_time = row[1]
                py_time = datetime.datetime.strptime(punch_time, "%I:%M %p").time()
                credible_id = row[2]
                action = row[5]
                paychex_type = row[6]
                hours = row[7]
                if action == 'Non-Work':
                    punch_type = paychex_type
                else:
                    punch_type = action
                if punch_type in type_map.keys():
                    punch_type_id = type_map[punch_type]
                else:
                    punch_type_id = 10
                if alg_utils.is_int(credible_id):
                    database_tasks.add_trivium_punch(
                        credible_id=credible_id,
                        punch_date=py_date.strftime('%Y%m%d'),
                        punch_time=py_time.strftime('%H%M%S'),
                        hours=hours,
                        punch_type_id=punch_type_id
                    )
                    logger.debug('parsed ' + str(skip) + ' / ' + str(row_count))
                    progress = round(float(skip)/float(row_count) * 100, 2)
                    print('\r' + str(progress) + '%', end='')
                else:
                    staff_name = str(row[3] + ' ' + row[4])
                    no_id.add(staff_name)
            skip += 1
    for staff_name in no_id:
        logger.warning('no credible id in paychex: ' + staff_name)


# verify that all employees in the spreadsheet have matched entries in the table##
# matches against credible by first and last name##
def check_employee_table(spreadsheet_name):
    logger.debug('started parsing employees from spreadsheet: '+spreadsheet_name)
    current_roster = database_tasks.get_current_trivium_roster()
    paychex_employees = parse_employees(spreadsheet_name)
    row_count = len(paychex_employees)
    progress = 0
    # get best guess credible id
    # check if entry present in current table
    # if not, add entry
    logger.debug('there are '+str(row_count) + ' in '+spreadsheet_name)
    for credible_id in paychex_employees.keys():
        paychex_employee = paychex_employees.get(credible_id)
        paychex_id = paychex_employee['paychex_id']
        current_last_name = paychex_employee['last_name']
        current_first_name = paychex_employee['first_name']
        if credible_id not in current_roster.keys():
            database_tasks.add_trivium_employee(
                credible_id=credible_id,
                paychex_id=paychex_id,
                last_name=current_last_name,
                first_name=current_first_name
            )
        else:
            paychex_hire_date = paychex_employee['hire_date']
            py_paychex_date = datetime.datetime.strptime(paychex_hire_date, '%m/%d/%Y')
            try:
                credible_hire_date = report_tasks.get_hire_date(credible_id)
                py_credible_date = datetime.datetime.strptime(credible_hire_date, '%m/%d/%Y')
                if py_paychex_date != py_credible_date:
                    logger.info('updated hire date for ' +
                                paychex_employee + ' to ' +
                                str(py_paychex_date) + ' from ' +
                                str(py_credible_date))
                    credible_tasks.update_employee_page(credible_id, 'hire_date', paychex_hire_date)
            except AttributeError:
                logger.warning('no hire date in Credible for '+str(paychex_employee))
                credible_tasks.update_employee_page(credible_id, 'hire_date', paychex_hire_date)

        progress += 1
        logger.debug('parsed ' + str(progress) + ' / ' + str(row_count))
    logger.debug('completed parsing records for spreadsheet: ' + spreadsheet_name)


def parse_employees(spreadsheet_name):
    progress = 0
    logger.debug('started parsing employees out of raw spreadsheet named: ' + spreadsheet_name)
    employees = {}
    with open(spreadsheet_name, 'r', encoding='UTF-8') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        row_count = sum(1 for _ in reader)
        logger.debug('there are  ' + str(row_count) + ' employees in ' + spreadsheet_name)
        csvfile.seek(0)
        for row in reader:
            if alg_utils.is_int(row[0]):
                credible_id = int(row[0])
                new_employee = {
                    'credible_id': credible_id,
                    'paychex_id': row[1],
                    'last_name': row[3],
                    'first_name': row[2],
                    'hire_date': row[5]
                }
                if credible_id not in employees.keys() and credible_id != '':
                    employees[credible_id] = new_employee
            progress += 1
            logger.debug('parsed ' + str(progress) + ' / ' + str(row_count))
    logger.debug('finished parsing spreadsheet: ' + spreadsheet_name)
    return employees


def parse_paychex_visits(spreadsheet_name):
    visit_types = set()
    punch_data = []
    with open(spreadsheet_name, 'r', encoding='UTF-8') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in reader:
            punch_row = {
                'date': row[0],
                'time': row[1],
                'paychex_id': row[2],
                'last_name': row[3],
                'first_name': row[4],
                'action': row[5],
                'type': row[6],
                'hours': row[7]
            }
            visit_types.add(row[6])
            punch_data.append(punch_row)
        print(punch_data)
