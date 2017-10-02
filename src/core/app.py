from celery import Celery
from celery.schedules import crontab
from fluent import sender, event

from . import utils

sender.setup(host='fluentd', port=24224, tag='alg.core.app')
event.Event('event', {
    'task': 'core',
    'info': {
        'message': 'started up the core application'
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

app = Celery('algernon',
             broker=RABBIT_URL,
             backend=REDIS_URL,
             include=[
                 'overwatch.tasks',
                 'overwatch.overwatch_tasks',
                 'overwatch.similarities',
                 'pump.pump_tasks'
             ])

app.conf.update(
    result_expires=3600,
)

app.conf.beat_schedule = {
    'lp_commsupt': {
        'task': 'overwatch.overwatch_tasks.lp_unapproved_commsupt',
        'schedule': crontab(minute='*/5'),
        'args': (),
    },
    'lp_clinical_team': {
        'task': 'overwatch.overwatch_tasks.lp_clinical_team',
        'schedule': crontab(minute='*/5'),
        'args': ()
    },
    'lp_discharge': {
        'task': 'overwatch.overwatch_tasks.lp_discharge',
        'schedule': crontab(minute='*/5'),
        'args': ()
    },
    'update_report_keys': {
        'task': 'overwatch.tasks.update_report_keys',
        'schedule': crontab(minute='*/10'),
        'args': ()
    },
    'create_pickles': {
        'task': 'overwatch.tasks.create_pickles',
        'schedule': crontab(hour=5, minute=16),
        'args': ()
    },
    'generate_audit_targets': {
        'task': 'overwatch.overwatch_tasks.select_audit_targets',
        'schedule': crontab(hour=5, minute=11, day_of_week='mon'),
        'args': ([5, ],)
    },
    'update_data_stores': {
        'task': 'overwatch.overwatch_tasks.update_pickle_datastore',
        'schedule': crontab(minute='*/15'),
        'args': ()
    }
}

if __name__ == '__main__':
    app.start()
