from celery import Celery
from flask import Flask, render_template, request, flash
from fluent import sender, event

from . import utils

sender.setup(host='fluentd', port=24224, tag='alg.api.app')

event.Event('event', {
        'task': 'app',
        'info': {
            'message': 'started up the main application for the workers'
        }
    })

RABBITMQ_USERNAME = utils.get_secret('rabbitmq', 'username')
RABBITMQ_PASSWORD = utils.get_secret('rabbitmq', 'password')
RABBITMQ_ADDRESS = utils.get_config('rabbit', 'address')
REDIS_ADDRESS = utils.get_config('redis', 'address')
REDIS_PASSWORD = utils.get_secret('redis', 'password')
REDIS_DATABASE = utils.get_config('redis', 'database')

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
celery_app = Celery('algernon', broker=RABBIT_URL, backend=REDIS_URL)
api.config.update(
    CELERY_BROKER_URL=RABBIT_URL,
    CELERY_RESULT_BACKEND=REDIS_URL
)


@api.route('/', methods=['GET'])
def home():
    return render_template('home.html')


@api.route('/pickles/data/<int:team_id>', methods=['GET', 'POST'])
def fire_team_datastore_update(team_id):
    task_id = celery_app.send_task('overwatch_tasks.update_single_pickle_datastore', args=[team_id])
    flash(task_id)
    print(task_id)
    return render_template('layout.html')


@api.route('/pickles/data/all', methods=['GET', 'POST'])
def fire_datastore_update():
    task_id = celery_app.send_task('overwatch_tasks.update_pickle_datastore', args=[])
    flash(task_id)
    return render_template('home.html')


@api.route('/update_emp_email', methods=['POST'])
def update_emp_email():
    data = request.json
    task_id = celery_app.send_task('overwatch_tasks.update_emp_email', args=[data['emp_id'], data['emp_email']])
    flash(task_id)
    return render_template('home.html')


@api.route('/vc/<string:email>')
def run_single_vc(email):
    task_id = celery_app.send_task('overwatch_tasks.vc_single', args=[email])
    flash(task_id)
    return render_template('home.html')


@api.route('/pickles/fire')
def trigger_pickles():
    task_id = celery_app.send_task('tasks.create_pickles', args=[])
    flash(task_id)
    return render_template('home.html')

if __name__ == '__main__':
    api.run(host='0.0.0.0', port=5000)
