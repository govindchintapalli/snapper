__author__ = 'govindchintapalli'


#datetime
CELERY_TIMEZONE = 'Asia/Kolkata'
CELERY_ENABLE_UTC = True

#tasks

CELERY_ANNOTATIONS = {'*': {'rate_limit': '10/s'}}

#concurrency

CELERYD_CONCURRENCY = 5
CELERYD_PREFETCH_MULTIPLIER = 2

#result_backend

CELERY_RESULT_BACKEND = 'amqp://10.1.10.14:5672'

#broker

BROKER_URL = 'amqp://guest:guest@10.1.10.14:5672'
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_ACCEPT_CONTENT = ['json']
