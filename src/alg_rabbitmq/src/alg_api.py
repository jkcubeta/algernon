from flask import Flask, render_template, json, request
from fluent import sender, event

from alg_py.alg_tasks import alg_utils
from alg_py.alg_tasks import report_tasks
from alg_py import overwatch_tasks, tasks

sender.setup(host='fluentd', port=24224, tag='alg.api.app')
event.Event('event', {
        'task': 'app',
        'info': {
            'message': 'started up the main application for the workers'
        }
    })

RABBITMQ_USERNAME = alg_utils.get_secret('rabbitmq', 'username')
RABBITMQ_PASSWORD = alg_utils.get_secret('rabbitmq', 'password')
RABBITMQ_ADDRESS = alg_utils.get_config('rabbit', 'address')
REDIS_ADDRESS = alg_utils.get_config('redis', 'address')
REDIS_PASSWORD = alg_utils.get_secret('redis', 'password')
REDIS_DATABASE = alg_utils.get_config('redis', 'database')

RABBIT_URL = 'amqp://' + RABBITMQ_USERNAME + ':' + RABBITMQ_PASSWORD + '@' + RABBITMQ_ADDRESS
REDIS_URL = 'redis://:' + REDIS_PASSWORD + '@' + REDIS_ADDRESS + REDIS_DATABASE

event.Event('event', {
        'task': 'app',
        'info': {
            'message': 'created the configurations for redis and rabbit',
            'params': {
                'rabbitmq_username': RABBITMQ_USERNAME,
                'rabbitmq_password': RABBITMQ_PASSWORD,
                'rabbitmq_address': RABBITMQ_ADDRESS,
                'redis_password': REDIS_PASSWORD,
                'redis_address': REDIS_ADDRESS,
                'redis_database': REDIS_DATABASE
            }
        }
    })


api = Flask(__name__)
api.config.update(
    CELERY_BROKER_URL=RABBIT_URL,
    CELERY_RESULT_BACKEND=REDIS_URL
)


@api.route('/', methods=['GET'])
def home():
    return render_template('home.html')


@api.route('/pickles/data', methods=['GET', 'POST'])
def fire_datastore_update():
    task_id = overwatch_tasks.update_pickle_datastore.delay()
    return task_id


@api.route('/update_emp_email', methods=['POST'])
def update_emp_email():
    data = request.json
    return overwatch_tasks.update_emp_email(data['emp_id'], data['emp_email'])


@api.route('/vc/<string:email>')
def run_single_vc(email):
    overwatch_tasks.vc_single.delay(email)


@api.route('/pickles/fire')
def trigger_pickles():
    tasks.create_pickles()


@api.route('/sheets/<int:report_id>/<int:param1>')
def csv_report(report_id, param1):
    data = report_tasks.get_google_formatted_report(report_id, [param1])


if __name__ == '__main__':
    api.run(host='0.0.0.0', port=5000)

