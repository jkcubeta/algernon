#!/usr/bin/env python3
from __future__ import unicode_literals

import datetime
import time

from celery import chord

from .alg_tasks import database_tasks
from .alg_tasks import credible_tasks
from .alg_tasks import gsuite_tasks
from .alg_tasks import medicaid_tasks
from .alg_tasks import parsing_tasks
from .alg_tasks import paychex_tasks
from .alg_tasks import pickle_tasks
from .alg_tasks import report_tasks
from .app import app


@app.task
def create_team_pickle(team_id):
    emp_roster = report_tasks.get_report_as_dict(302, [team_id])



@app.task()
def fix_null_punches():
    fixable_null_punches = paychex_tasks.get_fixable_null_punches()
    for emp_id in fixable_null_punches:
        fix_emp_null_punches.delay(fixable_null_punches[emp_id])


@app.task()
def fix_emp_null_punches(fixable_punch_dict):
    credible_tasks.update_admin_time_type_batch(fixable_punch_dict)


@app.task()
def check_for_unmigrated_punches():
    pending_punches = database_tasks.get_pending_punches()
    for emp_id in pending_punches:
        emp_punches = pending_punches[emp_id]
        migrate_single_employee_punches.delay(
            emp_id=emp_id,
            pending_punches=emp_punches
        )


@app.task()
def update_all_employee_records():
    benefits = report_tasks.get_benefits()
    current_profiles = report_tasks.get_report_as_dict(720, ['', '', ''])
    for emp_id in benefits:
        benefit_package = benefits.get(emp_id)
        current_profile = current_profiles.get(emp_id)
        for field in current_profile.keys():
            profile_field = current_profile.get(field)
            benefit_field = benefit_package.get(field)
            if profile_field != benefit_field:
                update_single_employee_record.delay(
                    emp_id=emp_id,
                    benefit_package=benefit_package
                )
                break


@app.task()
def update_single_employee_record(emp_id, benefit_package):
    update_data = {'text20': benefit_package['vacation_time'],
                   'text21': benefit_package['sick_time'],
                   'text22': benefit_package['personal_time'],
                   'text23': benefit_package['bereavement_time']
                   }
    credible_tasks.update_employee_page_batch(
        emp_id=emp_id,
        update_data=update_data
    )


@app.task()
def migrate_single_employee_punches(emp_id, pending_punches):
    paychex_tasks.migrate_single_employee_punches(
        emp_id=emp_id,
        pending_punches=pending_punches
    )


@app.task
def check_paychex_employees(spreadsheet_name):
    paychex_tasks.check_employee_table(spreadsheet_name)


@app.task
def check_punches():
    pending_punches = database_tasks.get_pending_punches()
    for emp_id in pending_punches:
        store_punches(emp_id, pending_punches)


@app.task
def store_punches(emp_id, pending_punches):
    checked_punches = []
    emp_pending_punches = pending_punches.get(emp_id)
    day_punches = paychex_tasks.extract_punches_by_day(emp_pending_punches)
    punches = paychex_tasks.parse_punches(day_punches)
    for punch in punches:
        date_in = punch['date']
        time_in = punch['time_in']
        admintime_id = report_tasks.get_admin_time_id(
            emp_id=emp_id,
            date_in=date_in,
            time_in=time_in
        )
        if admintime_id is 0:
            checked_punches.append(punch)
        else:
            parent_punch_ids = punch['parent_punch_ids']
            for parent_punch_id in parent_punch_ids:
                database_tasks.update_punch_admin_id(parent_punch_id, admintime_id)
    credible_tasks.add_admin_time_batch(
        emp_id=emp_id,
        punches=checked_punches)


@app.task
def build_credible_report(report_package):
    return pickle_tasks.build_credible_report(report_package)


def notify_of_pickle(email_address, workbook_id):
    sheet_stem = 'https://docs.google.com/spreadsheets/d/'
    subject = '[alg] reports created'
    text = 'hello, just wanted to let you know that i have made your reports and stored them in your drive. ' \
           '\nyou can get there from the link below. ' \
           '\nplease do not respond to this email, as i will ignore you. no offense, ' \
           'it is just how i am programmed. \nsincerely,\n' \
           'algernon moncrief, esq \n' + sheet_stem + str(workbook_id)
    gsuite_tasks.send_email(
        from_user='amoncrief@mbihs.com',
        from_user_decorated='Algernon Moncrief<amoncrief@mbihs.com>',
        to_user=email_address,
        subject=subject,
        text=text)


@app.task
def build_single_workbook(reports, receipt):
    email_address = reports['email_address']
    try:
        results = pickle_tasks.build_single_workbook(reports, receipt)
        notify_of_pickle(email_address, results['workbook_id'])
        return 'successfully built pickle for ' + email_address
    except Exception:
        return 'could not create pickle for email ' + email_address


@app.task
def display(results):
    return results


@app.task
def build_master_workbook(reports, mailing_list):
    receipt = pickle_tasks.build_master_workbook(reports)
    build_workbook_header = []
    for emp_id in mailing_list:
        mail_packet = mailing_list[emp_id]
        build_workbook_header.append(
            build_single_workbook.s(
                reports=mail_packet,
                receipt=receipt
            )
        )
    chord(build_workbook_header)(display.s())


@app.task
def create_pickles():
    lists = pickle_tasks.build_credible_lists()
    report_list = lists['report_list']
    mailing_list = lists['mailing_list']
    build_header = []
    for report_dict in report_list:
        build_header.append(build_credible_report.s(report_dict))
    chord(build_header)(build_master_workbook.s(mailing_list=mailing_list))


@app.task()
def update_report_keys():
    all_updated = credible_tasks.update_report_keys()
    while all_updated is 0:
        all_updated = credible_tasks.update_report_keys()


@app.task
def scavenge_unscrubbed():
    db = database_tasks.get_server_cursor_db()
    cursor = db.cursor()
    cursor.callproc('getUnfinishedScrubs')
    target = cursor.fetchone()
    while target is not None:
        credible_id = str(target[0])
        ts1 = time.time()
        start_time = datetime.datetime.fromtimestamp(
            ts1).strftime('%Y-%m-%d %H:%M:%S')
        scrub_id = database_tasks.add_scrub_stub(
            credible_id,
            start_time)
        scrub.delay(credible_id, scrub_id)
        target = cursor.fetchone()
    cursor.close()
    db.close()


@app.task
def pump(number_of_scrubs, number_of_days_stale):
    db = database_tasks.get_server_cursor_db()
    cursor = db.cursor()
    cursor.callproc('getEligibilityLimited', [number_of_scrubs])
    target = cursor.fetchone()
    while target is not None:
        credible_id = str(target[0])
        scrub_date = target[2]
        target = cursor.fetchone()
        conv_scrub_date = datetime.datetime.strptime(
            scrub_date,
            '%Y-%m-%d %H:%M:%S')
        today = datetime.datetime.fromtimestamp(time.time())
        trip_date = today.replace(day=today.day - int(number_of_days_stale))
        if conv_scrub_date <= trip_date:
            ts1 = time.time()
            start_time = datetime.datetime.fromtimestamp(
                ts1).strftime('%Y-%m-%d %H:%M:%S')
            scrub_id = database_tasks.add_scrub_stub(
                credible_id,
                start_time)
            print('new scrub_id is ' + str(scrub_id))
            scrub.delay(credible_id, scrub_id)

    cursor.close()
    db.close()


@app.task
def review_all_suggestions():
    db = database_tasks.get_server_cursor_db()
    cursor = db.cursor()
    cursor.callproc('getAllSuggestions')
    target = cursor.fetchone()
    while target is not None:
        review_suggestions.delay(target[0])
        target = cursor.fetchone()
    cursor.close()
    db.close()


@app.task
def update_client_roster():
    roster = set()
    credible = set()
    all_clients = credible_tasks.get_client_ids()
    for client in all_clients:
        credible.add(client)
    rows = database_tasks.get_current_roster()
    for row in rows:
        client_id = int(row[0])
        client_status = row[1]
        roster.add(client_id)
        if client_status != all_clients[client_id]:
            new_status = all_clients[client_id]
            database_tasks.update_client_status(client_id, new_status)
    if not credible <= roster:
        add_clients = credible - roster
        for client in add_clients:
            new_status = all_clients[client]
            database_tasks.add_client(client, new_status)


@app.task
def review_suggestions(credible_id):
    se = database_tasks.get_simple_eligibility(credible_id)
    suggestions = parsing_tasks.address_insurance_record(se)
    database_tasks.store_suggestions(credible_id, suggestions)


@app.task
def scrub(credible_id, scrub_id):
    html = medicaid_tasks.check_id(credible_id)
    ts2 = time.time()
    end_time = datetime.datetime.fromtimestamp(ts2).strftime('%Y-%m-%d %H:%M:%S')
    database_tasks.store_html(scrub_id, credible_id, end_time, html)
    if html:
        investigation_args = investigate(credible_id, scrub_id, html)
        # print(investigation_args)
        if investigation_args:
            suggestions = parsing_tasks.address_insurance_record(investigation_args)
            database_tasks.store_suggestions(credible_id, suggestions)
            # return([credible_id, suggestions])


@app.task
def check_all():
    app.control.purge()
    db = database_tasks.get_server_cursor_db()
    cursor = db.cursor()
    cursor.callproc('getAllEligibility')
    target = cursor.fetchone()
    while target is not None:
        client_id = str(target[0])
        print(client_id)
        ts1 = time.time()
        start_time = datetime.datetime.fromtimestamp(
            ts1).strftime('%Y-%m-%d %H:%M:%S')
        scrub_id = database_tasks.add_scrub_stub(
            client_id,
            start_time)
        scrub.delay(client_id, scrub_id)
        target = cursor.fetchone()
    cursor.close()
    db.close()


@app.task
def check_scrub_history():
    db = database_tasks.get_simple_db()
    cursor = db.cursor()
    cursor.callproc('getEligibilityLimited', ['50'])
    number = cursor.rowcount
    results = cursor.fetchall()
    today = datetime.datetime.fromtimestamp(time.time())
    trip_date = today.replace(day=today.day - 1)
    for row in results:
        scrub_date = row[2]
        conv_scrub_date = datetime.datetime.strptime(
            scrub_date,
            '%Y-%m-%d %H:%M:%S')
        client_id = str(row[0])
        if (conv_scrub_date < trip_date):
            ts1 = time.time()
            start_time = datetime.datetime.fromtimestamp(
                ts1).strftime('%Y-%m-%d %H:%M:%S')
            scrub_id = database_tasks.add_scrub_stub(
                client_id,
                start_time)
            print('new scrub_id is ' + str(scrub_id))
            scrub.delay(client_id, scrub_id)
    cursor.close()
    db.close()


@app.task
def investigate(client_id, scrub_id, html):
    clean_html = parsing_tasks.standardize(html)
    medicare = parsing_tasks.check_medicare(clean_html)
    medicaid = parsing_tasks.check_medicaid(clean_html)
    mco = parsing_tasks.check_mco(clean_html)
    tpl = parsing_tasks.check_tpl(clean_html)
    ltc = parsing_tasks.check_ltc(clean_html)
    desc = ''
    uninsured = not mco and not medicaid and not medicare and not tpl and not ltc
    if not mco and medicaid:
        desc = parsing_tasks.get_program_name(clean_html)
    if mco:
        desc = parsing_tasks.get_mco_name(clean_html)
    args = [client_id, scrub_id, medicaid, mco, medicare, uninsured, tpl, ltc, desc]
    # print('client_id, scrub_id, medicaid, mco, medicare, uninsured , tpl, ltc, description, html')
    database_tasks.add_eligibility(args)
    args.append(clean_html)
    return args
