import datetime

from pymongo import MongoClient

from . import alg_utils

mongo_address = alg_utils.get_config("mongodb", "address")
mongo_db = alg_utils.get_config("mongodb", "db_name")
overwatch_address = alg_utils.get_config("mongodb", "db_overwatch")
client = MongoClient(mongo_address)

db = client.get_database(mongo_db)
overwatch_db = client.get_database(overwatch_address)
overwatch_dbs = overwatch_db.collection_names()
if 'Service' not in overwatch_dbs:
    overwatch_db.create_collection('Service')
if 'Client' not in overwatch_dbs:
    overwatch_db.create_collection('Client')
if 'Staff' not in overwatch_dbs:
    overwatch_db.create_collection('Staff')
service_overwatch = db.Service
client_overwatch = db.Client
staff_overwatch = db.Staff
patients = db.Patient
encounters = db.Encounter
vc = db.VC


def check_log_for_action(record_type, pk_id, action):
    if record_type in 'staff':
        working_collection = staff_overwatch
    elif record_type in 'client':
        working_collection = client_overwatch
    else:
        working_collection = service_overwatch
    check = working_collection.find_one(
        {
            'pk_id': pk_id,
            'action': action
        }
    )
    if check:
        return True
    else:
        return False


def log_overwatch_action(record_type, pk_id, action, log_info):
    if record_type in 'staff':
        working_collection = staff_overwatch
    elif record_type in 'client':
        working_collection = client_overwatch
    else:
        working_collection = service_overwatch
    working_collection.insert_one(
        {
            'pk_id': pk_id,
            'change_date': datetime.datetime.now(),
            'action': action,
            'change_log': log_info
        }
    )


def store_email(email):
    vc.insert(email)


def get_patient_ids(last_name, first_name):
    results = patients.find({'last_name': last_name, 'first_name': first_name})
    patient_ids = []
    for result in results:
        patient_ids.append(result['_id'])
    return patient_ids


def get_most_recent_patient():
    top_results = []
    results = patients.find(
        {'$query': {}, '$orderby': {'_id': -1}}
    )
    for result in results:
        top_results.append(result['client_id'])
    if top_results:
        return top_results[0]
    else:
        return '0'


def get_most_recent_visit():
    top_results = []
    results = encounters.find(
        {'$query': {}, '$orderby': {'_id': -1}}
    )
    for result in results:
        top_results.append(result['clientvisit_id'])
    if top_results:
        return top_results[0]
    else:
        return '0'


def add_patient(patient_dict):
    patients.insert(patient_dict)


def add_encounter(visit_dict):
    encounters.insert(visit_dict)
