from pymongo import MongoClient

from . import alg_utils

mongo_address = alg_utils.get_config("mongodb", "address")
mongo_db = alg_utils.get_config("mongodb", "db_name")
client = MongoClient(mongo_address)

db = client.get_database(mongo_db)
patients = db.Patient
encounters = db.Encounter
vc = db.VC


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
