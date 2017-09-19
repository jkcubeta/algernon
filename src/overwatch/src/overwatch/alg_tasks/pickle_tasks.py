#!/usr/bin/env python3
import logging
import time

from fluent import event
from fluent import sender

from . import alg_utils
from . import database_tasks
from . import gsuite_tasks
from . import report_tasks

sender.setup(
    host=alg_utils.get_config('fluent', 'host'),
    port=alg_utils.get_config('fluent', 'port'),
    tag='alg.worker.pickles')
server_email = alg_utils.get_config('gsuite', 'server_account')


def create_team_book(team_id, team_name, template_file_id, template_sheets):
    requested_changes = []
    gsuite_tasks.create_workbook(server_email, team_name)
    team_bookfile = gsuite_tasks.get_file_by_name(server_email, team_name)
    summary_page = gsuite_tasks.fire_spreadsheet_copy(
        email=server_email,
        src_workbook_id=template_file_id,
        sheet_id=template_sheets['summary']['properties']['properties']['sheetId'],
        dst_workbook_id=team_bookfile['id']
    )
    requested_changes.append({
        'updateCells': {
            'rows': [
                {
                    'values': [
                        {
                            'userEnteredValue': {
                                'numberValue': team_id
                            }
                        }
                    ]
                }
            ],
            'fields': 'userEnteredValue.numberValue',
            'start': {
                'sheetId': summary_page['sheetId'],
                'rowIndex': 0,
                'columnIndex': 0
            }
        }
    })
    requested_changes.append({
        'updateSheetProperties': {
            'properties': {
                'sheetId': summary_page['sheetId'],
                'title': 'summary'
            },
            'fields': 'title'
        }
    })
    requested_changes.append({
        'deleteSheet': {
            'sheet_id': 0
        }
    })
    requested_changes.append({
        'addSheet': {
            'properties': {
                'title': 'csw'
            }
        }
    })
    requested_changes.append({
        'addSheet': {
            'properties': {
                'title': 'visits'
            }
        }
    })
    requested_changes.append({
        'addSheet': {
            'properties': {
                'title': 'clients'
            }
        }
    })
    return requested_changes


def update_team_book(team_id, team_name):
    requested_changes = []
    team_bookfile = gsuite_tasks.get_file_by_name(server_email, team_name)
    template_file_id = gsuite_tasks.get_file_by_name(server_email, 'Pickle Templates')['id']
    template_sheets = gsuite_tasks.get_sheets(template_file_id, server_email)
    if not team_bookfile:
        requested_changes = create_team_book(
            team_id=team_id,
            team_name=team_name,
            template_file_id=template_file_id,
            template_sheets=template_sheets
        )
        team_bookfile = gsuite_tasks.get_file_by_name(server_email, team_name)
    team_book_id = team_bookfile['id']
    team_sheets = gsuite_tasks.get_sheets(team_book_id, server_email)
    staff_roster = report_tasks.get_report_as_dict(302, [team_id])
    staff_names = []
    requested_copies = []
    for staff_id in staff_roster:
        if staff_roster[staff_id]['profile_code'] == 'CSW':
            staff_name = staff_roster[staff_id]['staff_name']
            staff_names.append(staff_name)
            if staff_name not in team_sheets.keys():
                requested_copies.append({
                    'email': server_email,
                    'src_workbook_id': template_file_id,
                    'src_sheet_id': template_sheets['caseload']['properties']['properties']['sheetId'],
                    'dst_workbook_id': team_book_id,
                    'title': staff_name,
                    'seed_value': staff_id
                })
    for sheet_name in team_sheets:
        if ',' in sheet_name:
            if sheet_name not in staff_names:
                requested_changes.append({
                    'deleteSheet': {
                        'sheet_id': team_sheets[sheet_name]['id']
                    }
                })
    if requested_copies:
        for copy in requested_copies:
            response = gsuite_tasks.fire_spreadsheet_copy(
                copy['email'],
                copy['src_workbook_id'],
                copy['dst_workbook_id'],
                copy['src_sheet_id']
            )
            new_sheet_id = response['sheetId']
            requested_changes.append({
                'updateSheetProperties': {
                    'properties': {
                        'sheetId': new_sheet_id,
                        'title': copy['title']
                    },
                    'fields': 'title'
                }
            })
            requested_changes.append({
                'updateCells': {
                    'rows': [
                        {
                            'values': [
                                {
                                    'userEnteredValue': {
                                        'numberValue': copy['seed_value']
                                    }
                                }
                            ]
                        }
                    ],
                    'fields': 'userEnteredValue.numberValue',
                    'start': {
                        'sheetId': new_sheet_id,
                        'rowIndex': 0,
                        'columnIndex': 0
                    }
                }
            })
    if requested_changes:
        gsuite_tasks.fire_batch_spreadsheet_change(server_email, spreadsheet_id=team_book_id, changes=requested_changes)


def get_datastore_report(team_id, sheet_type):
    report_map = {'visits': 776, 'csw': 302, 'clients': 303}
    report_data = report_tasks.get_google_formatted_report(report_map[sheet_type], [team_id])
    return {'range': sheet_type, 'values': report_data}


def update_data_store(results, team_name):
    value_changes = []
    ranges = []
    workbook_id = gsuite_tasks.get_file_by_name(server_email, team_name)['id']
    for result in results:
        if result:
            ranges.append(result['range'])
            value_changes.append(result)
    if value_changes:
        gsuite_tasks.fire_batch_value_clear(server_email, workbook_id, ranges)
        gsuite_tasks.fire_batch_values_change(server_email, workbook_id, value_changes)


def build_credible_report(report_package):
    event.Event('event', {
        'pickle_task': 'started building credible report',
        'report_package': str(report_package)
    })
    key_id = report_package['key_id']
    params = [
        report_package['param1'],
        report_package['param2'],
        report_package['param3'],
    ]
    logging.debug('starting report for key id: ' + str(key_id) +
                  'with params: ' + str(params))
    report_data = report_tasks.get_google_formatted_report(
        report_id=key_id,
        params=params,
    )
    report_tag = (
        str(key_id) + '_' +
        str(report_package['param1']) + '_' +
        str(report_package['param2']) + '_' +
        str(report_package['param3'])
    )
    event.Event('event', {
        'task': 'pickle_tasks',
        'info': {
            'message': 'finished building credible report',
            'report_pacakge': str(report_package)
        }
    })
    return {
        'report_tag': report_tag,
        'report_data': report_data}


def build_credible_reports(report_list):
    reports = {}
    event.Event('event', {
        'pickle_task': 'building credible reports',
        'params': str(report_list),
        'length': len(report_list)
    })
    for report in report_list:
        key_id = report['key_id']
        params = [
            report['param1'],
            report['param2'],
            report['param3'],
        ]
        event.Event('event', {
            'pickle_task': 'started building a credible report',
            'params': str(params)
        })
        report_data = report_tasks.get_google_formatted_report(
            report_id=key_id,
            params=params,
        )
        report_tag = (
            str(key_id) + '_' +
            str(report['param1']) + '_' +
            str(report['param2']) + '_' +
            str(report['param3'])
        )
        reports[report_tag] = report_data
        event.Event('event', {
            'pickle_task': 'finished building credible report',
            'params': str(params)
        })
    event.Event('event', {
        'pickle_task': 'completed building a credible reports',
        'params': str(report_list),
        'length': len(report_list)
    })
    return reports


def build_credible_lists():
    event.Event('event', {
        'pickle_task': 'started credible lists for the mailer'
    })
    report_list = []
    email_list = {}
    mailing_lists = database_tasks.get_mailing_list()
    for emp_id in mailing_lists:
        email_address = report_tasks.get_emp_email(emp_id)
        mailed_reports = mailing_lists.get(emp_id)
        report_keys = []
        for mailed_report in mailed_reports:
            report_key = (
                str(mailed_report['key_id']) + '_' +
                str(mailed_report['param1']) + '_' +
                str(mailed_report['param2']) + '_' +
                str(mailed_report['param3'])
            )
            report_keys.append(report_key)
        email_list[emp_id] = {
            'email_address': email_address,
            'reports': report_keys,
        }
        for mailed_report in mailed_reports:
            if mailed_report not in report_list:
                report_list.append(mailed_report)
    event.Event('event', {
        'pickle_task': 'built credible lists for the mailer'
    })
    return {
        'report_list': report_list,
        'mailing_list': email_list,
    }


def build_master_workbook(reports):
    event.Event('event', {
        'task': 'pickle_tasks',
        'info': {
            'message': 'started building master workbook',
            'reports': str(reports)
        }
    })
    receipt = {}
    reports_receipt = {}
    workbook_name = time.strftime("%x") + '_daily report'
    workbook_id = gsuite_tasks.create_workbook(
        email=server_email,
        workbook_name=workbook_name
    )
    for report in reports:
        event.Event('event', {
            'task': 'pickle_tasks',
            'info': {
                'message': 'started building master workbook worksheet',
                'report': str(report)
            }
        })
        report_tag = report['report_tag']
        sheet_name = get_sheet_name(report_tag)
        data = report['report_data']
        gsuite_tasks.create_sheet(
            email=server_email,
            workbook_id=workbook_id,
            sheet_name=sheet_name
        )
        gsuite_tasks.write_values(
            email=server_email,
            workbook_id=workbook_id,
            sheet_name=sheet_name,
            sheet_data=data,
        )
        reports_receipt[report_tag] = sheet_name
        event.Event('event', {
            'task': 'pickle_tasks',
            'info': {
                'message': 'finished building master workbook',
                'report': str(report),
                'sheet_name': sheet_name
            }
        })
    gsuite_tasks.delete_first_sheet_id(
        email=server_email,
        workbook_id=workbook_id
    )
    receipt['master_workbook_id'] = workbook_id
    receipt['report_receipt'] = reports_receipt
    event.Event('event', {
        'task': 'pickle_tasks',
        'info': {
            'message': 'finished building master workbook',
            'reports': str(reports)
        }
    })
    return receipt


def build_single_workbook(reports, receipt):
    event.Event('event', {
        'pickle_task': 'started building user workbook',
        'report_package': str(reports)
    })
    email = reports['email_address']
    ordered_report_tags = reports['reports']
    workbook_name = time.strftime("%x") + '_daily report'
    master_workbook_id = receipt['master_workbook_id']
    report_receipt = receipt['report_receipt']
    workbook_id = gsuite_tasks.create_workbook(
        email=email,
        workbook_name=workbook_name
    )
    for report_tag in ordered_report_tags:
        sheet_name = report_receipt.get(report_tag)
        values = gsuite_tasks.get_sheet_values(
            email=server_email,
            workbook_id=master_workbook_id,
            sheet_name=sheet_name
        )
        gsuite_tasks.create_sheet(
            email=email,
            workbook_id=workbook_id,
            sheet_name=sheet_name
        )
        gsuite_tasks.write_values(
            email=email,
            workbook_id=workbook_id,
            sheet_name=sheet_name,
            sheet_data=values
        )
    event.Event('event', {
        'pickle_task': 'finished building user workbook',
        'report_package': str(reports)
    })
    return {'message': email + ' mailed successfully', 'workbook_id': workbook_id}


def get_sheet_name(report_tag):
    report_parts = report_tag.split('_')
    report_id = report_parts[0]
    param1 = report_parts[1]
    param2 = report_parts[2]
    param3 = report_parts[3]
    common_name = database_tasks.get_report_common_name(report_id)
    sheet_name = common_name + ' ' + param1 + ' ' + param2 + ' ' + param3
    return sheet_name
