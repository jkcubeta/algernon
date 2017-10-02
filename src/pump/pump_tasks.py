import pymongo
from fluent import sender, event
from pymongo import errors
from core.app import app

from arch.obj.Patient import Patient, PatientDemographic, PatientDemographics, PatientData, PatientDataField
from pump.Credible import CredibleReport
from pump.GSuite import EmailService
from pump import utils

client = pymongo.MongoClient('192.168.66.9', port=1111)
arch = client.get_database('arch')
vc = client.get_database('vc')
patient_collection = arch.Patient
da_collection = arch.DA
vce = vc.VCE

sender.setup(
    host=utils.get_config('fluent', 'host'),
    port=utils.get_config('fluent', 'port'),
    tag='alg.worker.pumpp')


def get_all_ids(collection):
    ids = collection.find(
        {},
        {'patient_data.credible_demographics.client_id': 1, '_id': 0})
    client_ids = []
    for client_package in ids:
        client_id = client_package['patient_data']['credible_demographics']['client_id']['primary']
        client_ids.append(client_id)
    return client_ids


def get_top_id(collection):
    ids = get_top_id(collection)
    top_id = ids[-1]['patient_data']['credible_demographics']['client_id']['primary']
    return top_id


@app.task
def pump_vc(email_address):
    messages = []
    email_service = EmailService(email_address)
    email_results = email_service.service.users().messages().list(userId=email_address).execute()
    for result in email_results['messages']:
        message = email_service.service.users().messages().get(userId=email_address, id=result['id'], format='full').execute()
        messages.append(message)
    vce.insert_many(messages)


@app.task
def review_old_patients():
    current_credible_ids = get_all_ids(patient_collection)
    for credible_id in current_credible_ids:
        try:
            patient_collection.find_one(
                {'patient_data.credible_demographics.client_id.primary': int(credible_id)},
                {'_id': 1})['_id']
        except TypeError:
            patient = CredibleReport.from_sql('SELECT * FROM Clients WHERE client_id = ' + str(credible_id))
            add_patient.delay(patient)


@app.task
def add_patient(patient):
    alg_patient = Patient(patient['first_name'], patient['last_name'], patient['dob'])
    demographics = PatientDemographics()
    for field_name in patient:
        field_value = patient[field_name]
        demographic = PatientDemographic(field_value)
        demographics.add_demographic(field_name, demographic)
    alg_patient.add('credible_demographics', demographics)
    event.Event('event', {
        'task': 'pump_task',
        'info': {
            'message': 'built a new algernon patient from credible for id ' + str(patient['credible_id'])
        }
    })
    try:
        patient_collection.insert_one(alg_patient.to_dict())
    except errors.DuplicateKeyError:
        pass


@app.task
def pump_new_patients():
    event.Event('event', {
        'task': 'pump_task',
        'info': {
            'message': 'started patient pump'
        }
    })
    top_id = get_top_id(patient_collection)
    event.Event('event', {
        'task': 'pump_task',
        'info': {
            'message': 'top id in our system is ' + str(top_id)
        }
    })
    patients = CredibleReport.from_sql('Select * FROM Clients WHERE client_id > ' + str(top_id))
    event.Event('event', {
        'task': 'pump_task',
        'info': {
            'message': 'retrieved ' + str(len(patients.__dict__)) + ' new patients from credible'
        }
    })
    for patient_id in patients:
        patient = patients[patient_id]
        add_patient.delay(patient)


@app.task
def pump_da(client_id):
    event.Event('event', {
        'task': 'pump_task.pump_da',
        'info': {
            'message': 'started pumping the DA for client id ' + str(client_id)
        }
    })
    questions = []
    mongo_id = patient_collection.find_one(
        {'patient_data.credible_demographics.client_id.primary': int(client_id)},
        {'_id': 1})['_id']
    event.Event('event', {
        'task': 'pump_task',
        'info': {
            'message': 'mongo id is ' + str(mongo_id) + ' for credible id ' + str(client_id)
        }
    })
    patient_data = PatientData()
    report_second = CredibleReport.from_report(792, [client_id])
    report = CredibleReport.from_report(791, [client_id])
    for question_id in report_second:
        try:
            question_text = report_second[question_id]['question_text']
        except TypeError:
            question_text = report_second[question_id][0]['question_text']
        questions.append({'question_id': question_id, 'question_text': question_text})
    for question_package in questions:
        question_id = question_package['question_id']
        question_text = question_package['question_text']
        try:
            report_answer = report[question_id]
        except KeyError:
            report_answer = None
        try:
            report_second_answer = report_second[question_id]
        except KeyError:
            report_second_answer = None
        if report_answer:
            da_data = report_answer['answer_note']
        else:
            try:
                da_data = report_second_answer['answer']
            except TypeError:
                multiple_answer = []
                for answer in report_second_answer:
                    multiple_answer.append(answer['answer'])
                da_data = multiple_answer
        patient_data_field = PatientDataField(da_data)
        patient_data.add_data(question_text, patient_data_field)
    patient_collection.update_one(
        {'_id': mongo_id},
        {"$set": {'patient_data.credible_da': patient_data.to_dict()}})
