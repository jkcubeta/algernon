import ast
import logging

from . import utils
import httplib2
from fluent import sender, event
from googleapiclient.discovery import Resource
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from oauth2client.service_account import ServiceAccountCredentials
from retrying import retry

CREDENTIALS = {
    'drive': {
        'key': utils.get_secret('gsuite', 'drive_robot_keyfile'),
        'scopes': ['https://www.googleapis.com/auth/drive']
    },
    'gmail': {
        'key': utils.get_secret('gsuite', 'gmail_robot_keyfile'),
        'scopes': ['https://www.googleapis.com/auth/gmail.modify']
    },
    'admin': {
        'key': utils.get_secret('gsuite', 'admin_robot_keyfile'),
        'scopes': ['https://www.googleapis.com/auth/admin']
    }
}


logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
server_email = utils.get_config('gsuite', 'server_account')
discovery_url = 'https://www.googleapis.com/discovery'


def get_credentials(credential_type):
    scopes = CREDENTIALS[credential_type]['scopes']
    key_path = CREDENTIALS[credential_type]['key']
    keyfile_string = open(key_path).read()
    keyfile_dict = ast.literal_eval(keyfile_string)
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(keyfile_dict, scopes)
    return credentials


def get_authorized_http(credential_type, email=server_email):
    credentials = get_credentials(credential_type)
    delegated_credentials = credentials.create_delegated(email)
    http = httplib2.Http()
    return delegated_credentials.authorize(http)


class User:
    def __init__(self, email):
        self.email = email


class EmailService:
    def __init__(self, email):
        http = get_authorized_http('gmail', email)
        service = build('gmail',
                        'v1',
                        http=http)
        self.service = service


class Email:
    def __init__(self, from_user, to_user, subject, message, sent_time):
        self.data = {
            'to_user': to_user,
            'from_user': from_user,
            'subject': subject,
            'message': message,
            'sent_time': sent_time
        }

    @classmethod
    def from_message(cls, message):
        items = message.items()
        to_user_package = items[0]
        from_user_package = items[19]
        date_package = items[20]