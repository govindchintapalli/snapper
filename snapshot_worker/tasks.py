import subprocess
from celery import group
from snapshot_worker.celery_worker import worker
from snapshot_worker.config import tables, db_config, app_config
from snapshot_worker.snapper.snaptasks import PGFetchTableTask, PGLoadTableTask

__author__ = 'govindchintapalli'

PGFetchTableTask.bind(worker)
PGLoadTableTask.bind(worker)

fetch = PGFetchTableTask(db_config=db_config.config)
load = PGLoadTableTask(db_config=db_config.config)


@worker.task()
def clean(path):
    return subprocess.call('/usr/bin/rm -fr {}'.format(path), shell=True)


@worker.task()
def snap(source_db_name, target_db_name, table_name):
    file_path = app_config.TEMPORARY_FILE_PATH + table_name
    fetch.apply(db_config.config[source_db_name], table_name, file_path)
    load.apply(db_config.config[target_db_name], file_path)
    clean.apply(file_path).delay()


@worker.task()
def base_backup(backup_path, db):
    subprocess.check_call('PGPASSWORD="{PASSWORD}" /usr/pgsql-9.3/bin/pg_basebackup -D \
    {0} -p {PORT} -U {USER} -h {HOST}'.format(backup_path, **db), shell=True)


@worker.task()
def snap_all_tables():
    group(snap.s(*i) for i in tables.list).apply_async()


@worker.task()
def complete_snapshot(backup_path, db):
    base_backup.apply(backup_path, db)
    snap_all_tables.apply()