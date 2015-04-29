from celery import Celery
from snapshot_worker.config import celery_config, app_config

__author__ = 'govindchintapalli'

worker = Celery(app_config.APP_NAME)
worker.config_from_object(celery_config)
