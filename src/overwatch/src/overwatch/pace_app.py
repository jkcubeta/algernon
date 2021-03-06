from celery import Celery
from celery.schedules import crontab
from fluent import sender, event

from .alg_tasks import alg_utils

sender.setup(host='fluentd', port=24224, tag='alg.api.app')
event.Event('event', {
        'task': 'app',
        'info': {
            'message': 'started up the main application for the api eye'
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

app = Celery('alg_py',
             broker=RABBIT_URL,
             backend=REDIS_URL,
             include=['alg_py.tasks', 'alg_py.overwatch_tasks', 'alg_py.similarities', 'alg_py.algernon'])

app.conf.update(
    result_expires=3600,
)

app.conf.beat_schedule = {
    'lp_commsupt': {
        'task': 'alg_py.overwatch_tasks.lp_unapproved_commsupt',
        'schedule': crontab(minute='*/5'),
        'args': (),
    },
    'lp_clinical_team': {
        'task': 'alg_py.overwatch_tasks.lp_clinical_team',
        'schedule': crontab(minute='*/5'),
        'args': ()
    },
    'update_report_keys': {
        'task': 'alg_py.tasks.update_report_keys',
        'schedule': crontab(minute='*/10'),
        'args': ()
    },
    'create_pickles': {
        'task': 'alg_py.tasks.create_pickles',
        'schedule': crontab(hour=5, minute=16),
        'args': ()
    },
}

event.Event('event', {
        'task': 'app',
        'info': {
            'message': 'finished the start up the main application for the api eye'
        }
    })

if __name__ == '__main__':
    app.start()



