#!/usr/bin/env python3
from __future__ import print_function

import ast
import base64
import logging
from pprint import pprint
from email.mime.text import MIMEText

import httplib2
from fluent import sender, event
from googleapiclient.discovery import Resource
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from oauth2client.service_account import ServiceAccountCredentials
from retrying import retry

from . import alg_utils

CREDENTIALS = {
    'drive': {
        'key': alg_utils.get_secret('gsuite', 'drive_robot_keyfile'),
        'scopes': ['https://www.googleapis.com/auth/drive']
    },
    'gmail': {
        'key': alg_utils.get_secret('gsuite', 'gmail_robot_keyfile'),
        'scopes': ['https://www.googleapis.com/auth/gmail.modify']
    },
    'admin': {
        'key': alg_utils.get_secret('gsuite', 'admin_robot_keyfile'),
        'scopes': ['https://www.googleapis.com/auth/admin']
    }
}

logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
server_email = alg_utils.get_config('gsuite', 'server_account')
discovery_url = 'https://www.googleapis.com/discovery'
drive_discovery_url = 'https://www.googleapis.com/discovery/v1/apis/drive/v3/rest'
sender.setup(
    host=alg_utils.get_config('fluent', 'host'),
    port=alg_utils.get_config('fluent', 'port'),
    tag='alg.worker.pickles')


def fire_batch_value_clear(email: str, spreadsheet_id: str, ranges: list):
    http = get_authorized_http('drive', email=email)
    body = {'ranges': ranges}
    return build('sheets', 'v4', http=http) \
        .spreadsheets() \
        .values() \
        .batchClear(spreadsheetId=spreadsheet_id, body=body) \
        .execute()


@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=25)
def fire_batch_values_change(email: str, spreadsheet_id: str, changes: list({str: object})):
    http = get_authorized_http('drive', email=email)
    body = {
        'valueInputOption': 'USER_ENTERED',
        'data': changes}
    pprint(body)
    build('sheets', 'v4', http=http) \
        .spreadsheets()\
        .values()\
        .batchUpdate(spreadsheetId=spreadsheet_id, body=body) \
        .execute()


@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=25)
def fire_batch_spreadsheet_change(email: str, spreadsheet_id: str, changes: dict({str: object})):
    http = get_authorized_http('drive', email=email)
    body = {'requests': changes}
    service = build('sheets', 'v4', http=http)\
        .spreadsheets()\
        .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
    service.execute()


@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=25)
def fire_spreadsheet_copy(email: str,src_workbook_id: str, dst_workbook_id: str, sheet_id: str):
    http = get_authorized_http('drive', email=email)
    body = {'destinationSpreadsheetId': dst_workbook_id}
    service = build('sheets', 'v4', http=http) \
        .spreadsheets()\
        .sheets()\
        .copyTo(spreadsheetId=src_workbook_id, sheetId=sheet_id, body=body)
    return service.execute()


def get_email_request(email):
    http = get_authorized_http('gmail', email)
    service = build('gmail',
                    'v1',
                    http=http)
    return service


def refresh_data_sheet(email, book_id, sheet_name, values):
    sheet_metadata = get_sheet_metadata(book_id, email, sheet_name)
    while not sheet_metadata:
        create_sheet(email, book_id, sheet_name)
        sheet_metadata = get_sheet_metadata(book_id, email, sheet_name)
    clear_sheet(email, book_id, sheet_metadata['sheetId'])
    write_values(email, book_id, sheet_name, values)


def clear_sheet(email, book_id, sheet_id):
    body = {
        'requests': [
            {
                'deleteRange': {
                    'range': {
                        'sheetId': sheet_id
                    },
                    'shiftDimension': "ROWS"
                }
            }
        ]
    }
    service = build("sheets", "v4", http=get_authorized_http('drive', email)). \
        spreadsheets(). \
        batchUpdate(spreadsheetId=book_id, body=body)
    service.execute()


def get_file_by_name(email, file_name):
    files = get_all_files(email)
    possibles = []
    for file in files:
        name = file['name']
        if name == file_name:
            possibles.append(file)
    if not possibles or len(possibles) > 1:
        return None
    else:
        return possibles[0]


def get_all_files(email):
    http = get_authorized_http('drive', email)
    service = build('drive', 'v3', http=http)
    files = service.files().list().execute()
    return files['files']


def get_authorized_http(credential_type, email=server_email):
    credentials = get_credentials(credential_type)
    delegated_credentials = credentials.create_delegated(email)
    http = httplib2.Http()
    return delegated_credentials.authorize(http)


def get_gsuite_group_info():
    http = get_authorized_http('admin')
    service = build("admin", "directory_v1", http=http)
    return service.groups().list(domain='mbihs.com').execute()


def get_emp_group(user_email):
    """
   Return list of groups user belongs to
   :param user_email: string
   :return: [string, string2, ...]
   """
    group_email = []
    http = get_authorized_http('admin')
    service = build("admin", "directory_v1", http=http)
    group_dict = service.groups().list(userKey=user_email).execute()
    if 'groups' in group_dict:
        group_list = group_dict['groups']
        for dictionary in group_list:
            email = dictionary['email']
            group_email.append(email)
    return group_email


def add_google_group_user(user_email, team_email, is_admin):
    """Match user to google group based on team, as a member
   """
    body = {
        "role": "MEMBER",  # Role of member
        "email": user_email,  # Email of member (Read-only)
    }
    http = get_authorized_http('admin')
    service = build("admin", "directory_v1", http=http)
    if is_admin == 'true':
        body['role'] = "OWNER"
    service.members().insert(groupKey=team_email, body=body).execute()


def get_group_name_map():
    test = get_gsuite_group_info()
    test_dict = test['groups']
    gsuite_map = {}
    for group in test_dict:
        email = group['email']
        team_name = group['name']
        gsuite_map[team_name] = email
    return gsuite_map


def check_user(user_email):
    http = get_authorized_http('admin')
    service = build("admin", "directory_v1", http=http)
    try:
        service.users().get(userKey=user_email).execute()
        return True
    except HttpError:
        return False


def get_all_emails():
    http = get_authorized_http('gmail')
    service = build("admin", "directory_v1", http=http)
    return service.users().list(domain='mbihs.com', maxResults=500, pageToken=None).execute()


def send_email_to_multiple(users, subject, text):
    for user in users:
        send_email(server_email, user, subject, text)


def send_email(from_user, to_user, subject, text, from_user_decorated=None):
    if not from_user_decorated:
        from_user_decorated = from_user
    message = MIMEText(text)
    message['to'] = to_user
    message['from'] = from_user_decorated
    message['subject'] = subject
    raw = base64.urlsafe_b64encode(message.as_bytes())
    raw = raw.decode()
    body = {'raw': raw}
    http = get_authorized_http('gmail', from_user)
    service = build('gmail',
                    'v1',
                    http=http)
    service.users().messages().send(userId=from_user, body=body).execute()


def copy_sheet(email, master_sheet_id, master_workbook_id, target_workbook_id):
    body = {
        'destinationSpreadsheetId': target_workbook_id
    }
    http = get_authorized_http('drive', email)
    service = build("sheets",
                    "v4",
                    http=http,
                    discoveryServiceUrl=discovery_url)
    service.spreadsheets().sheets().copyTo(
        spreadsheetId=master_workbook_id,
        sheetId=master_sheet_id,
        body=body,
    ).execute()


def delete_first_sheet_id(email, workbook_id):
    event.Event('event', {
        'gsuite_task': 'started the deletion of the first workbook sheet',
        'workbook_id': str(workbook_id),
        'email': str(email)
    })
    delete_sheet(email, workbook_id, 0)
    event.Event('event', {
        'gsuite_task': 'completed the deletion of the first workbook sheet',
        'workbook_id': str(workbook_id),
        'email': str(email)
    })


def delete_sheet(email, workbook_id, sheet_id):
    body = {
        'requests': [
            {
                'deleteSheet': {
                    'sheet_id': sheet_id
                }
            },
        ]
    }
    http = get_authorized_http('drive', email)
    service = build("sheets",
                    "v4",
                    http=http,
                    discoveryServiceUrl=discovery_url)
    service.spreadsheets().batchUpdate(
        spreadsheetId=workbook_id,
        body=body,
    ).execute()


def request_add_sheet_create(request_body, sheet_name):
    request_body['requests'].append(
        {
            'addSheet': {
                'properties': {
                    'title': sheet_name
                }
            }
        }
    )
    return request_body


def request_add_sheet_delete(request_body, sheet_id):
    request_body['requests'].append(
        {
            'deleteSheet': {
                'sheet_id': sheet_id
            }
        }
    )
    return request_body


def create_sheet(email, workbook_id, sheet_name):
    event.Event('event', {
        'gsuite_task': 'started the creation of a workbook sheet',
        'workbook_id': str(workbook_id),
        'email': str(email),
        'sheet_name': str(sheet_name)
    })
    body = {
        'requests': [
            {
                'addSheet': {
                    'properties': {
                        'title': sheet_name
                    }
                }
            },
        ]
    }
    http = get_authorized_http('drive', email)
    service = build("sheets",
                    "v4",
                    http=http,
                    discoveryServiceUrl=discovery_url)
    test = service.spreadsheets().batchUpdate(
        spreadsheetId=workbook_id,
        body=body,
    ).execute()
    sheet_id = test['replies'][0]['addSheet']['properties']['sheetId']
    event.Event('event', {
        'gsuite_task': 'finished the creation of a workbook sheet',
        'workbook_id': str(workbook_id),
        'email': str(email),
        'sheet_name': str(sheet_name),
        'written sheet id': str(sheet_id)
    })
    return sheet_id


def write_values(email, workbook_id, sheet_name, sheet_data):
    event.Event('event', {
        'gsuite_task': 'started writing values to a workbook sheet',
        'workbook_id': str(workbook_id),
        'email': str(email),
        'sheet_name': str(sheet_name),
        'sheet_data': str(sheet_data)
    })
    body = {
        'valueInputOption': 'USER_ENTERED',
        'data': {
            'range': sheet_name,
            'values': sheet_data
        }
    }
    http = get_authorized_http('drive', email)
    service = build("sheets",
                    "v4",
                    http=http,
                    discoveryServiceUrl=discovery_url)
    test = service.spreadsheets().values().batchUpdate(
        spreadsheetId=workbook_id,
        body=body,
    ).execute()
    event.Event('event', {
        'gsuite_task': 'finished writing values to a workbook sheet',
        'workbook_id': str(workbook_id),
        'email': str(email),
        'sheet_name': str(sheet_name),
        'sheet_data': str(sheet_data),
        'write return': str(test)
    })


def create_workbook(email, workbook_name):
    event.Event('event', {
        'task': 'gsuite_tasks',
        'info': {
            'message': 'started creating a workbook',
            'email': str(email),
            'workbook_name': str(workbook_name)
        }
    })
    body = {
        'mimeType': 'application/vnd.google-apps.spreadsheet',
        'name': workbook_name,
    }
    http = get_authorized_http('drive', email)
    service = build("drive",
                    "v3",
                    http=http,
                    discoveryServiceUrl=drive_discovery_url)
    response = service.files().create(body=body).execute()
    workbook_id = response['id']
    event.Event('event', {
        'task': 'gsuite_tasks',
        'info': {
            'message': 'finished creating a workbook',
            'email': str(email),
            'workbook_name': str(workbook_name),
            'new_workbook_id': str(workbook_id)
        }
    })
    return workbook_id


def get_sheets(spreadsheet_id: str, email: str) -> dict({str: Resource}):
    sheets = {}
    sheet_names = []
    spreadsheet = get_spreadsheet(spreadsheet_id, email=email)
    for sheet in spreadsheet['sheets']:
        sheet_name = sheet['properties']['title']
        sheets[sheet_name] = {'properties': sheet}
        sheet_names.append(sheet_name)
    values = get_spreadsheet_values(spreadsheet_id, email, sheet_names)
    for sheet in values['valueRanges']:
        title = sheet['range'].split('!')[0].replace("'", '')
        sheets[title]['values'] = sheet
    return sheets


def get_spreadsheet_values(spreadsheet_id, email, ranges):
    http = get_authorized_http('drive', email)
    service = build("sheets", "v4", http=http).spreadsheets()\
        .values()\
        .batchGet(spreadsheetId=spreadsheet_id, ranges=ranges)
    return service.execute()



def get_sheet_update(email):
    http = get_authorized_http('drive', email)
    service = build("sheets", "v4", http=http).batchUpdate()
    return service


def get_spreadsheet(spreadsheet_id, email):
    http = get_authorized_http('drive', email)
    service = build("sheets", "v4", http=http).spreadsheets().get(spreadsheetId=spreadsheet_id)
    spreadsheet = service.execute()
    return spreadsheet


def get_sheet_metadata(spreadsheet_id, email, sheet_name):
    spreadsheet = get_spreadsheet(spreadsheet_id, email)
    sheets = spreadsheet['sheets']
    for sheet in sheets:
        sheet_properties = sheet['properties']
        if sheet_properties['title'] == sheet_name:
            return sheet_properties
    return None


def get_sheet_values(email, workbook_id, sheet_name):
    event.Event('event', {
        'gsuite_task': 'started the extraction of workbook data',
        'workbook_id': str(workbook_id),
        'email': str(email),
        'sheet name': str(sheet_name)
    })
    http = get_authorized_http('drive', email)
    service = build("sheets",
                    "v4",
                    http=http,
                    discoveryServiceUrl=discovery_url)
    response = service.spreadsheets().values().get(
        spreadsheetId=workbook_id,
        range=str(sheet_name),
    ).execute()
    values = response['values']
    event.Event('event', {
        'gsuite_task': 'completed the extraction of workbook data',
        'workbook_id': str(workbook_id),
        'email': str(email),
        'sheet name': str(sheet_name)
    })
    return values


# noinspection PyTypeChecker
def get_credentials(credential_type):
    scopes = CREDENTIALS[credential_type]['scopes']
    key_path = CREDENTIALS[credential_type]['key']
    keyfile_string = open(key_path).read()
    keyfile_dict = ast.literal_eval(keyfile_string)
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(keyfile_dict, scopes)
    return credentials
