from snapshot_worker import tasks
from config import app_config

__author__ = 'govindchintapalli'


tasks.complete_snapshot.delay(app_config.BASE_BACKUP_PATH, app_config.ANALYTICS_SERVER)