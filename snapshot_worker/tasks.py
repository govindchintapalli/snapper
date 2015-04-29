import subprocess
import os
import fnmatch
from celery import group, chain
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
    chain(fetch.si(db_config.config[source_db_name], table_name, file_path),
            load.si(db_config.config[target_db_name], file_path),
            clean.si(file_path)).delay()

@worker.task()
def cp_files(source_dir, pattern, destination_dir):
    for file_name in os.listdir(source_dir):
        if fnmatch.fnmatch(file_name, pattern):
            subprocess.check_call('cp {conf_file} {target}'.format(conf_file=file_name, target=destination_dir), shell=True)


@worker.task()
def base_backup(backup_path, db):
    subprocess.check_call('/etc/init.d/postgresql-9.3 stop')
    cp_files.apply(app_config.CONF_DIR,app_config.CONF_MATCH,app_config.CONF_DST_DIR)
    subprocess.check_call('rm -fr {data_dir}'.format(data_dir=app_config.BASE_BACKUP_PATH), shell=True)
    subprocess.check_call('PGPASSWORD="{PASSWORD}" /usr/pgsql-9.3/bin/pg_basebackup -D \
    {0} -p {PORT} -U {USER} -h {HOST}'.format(backup_path, **db), shell=True)
    cp_files.apply(app_config.CONF_DST_DIR,app_config.CONF_MATCH, app_config.CONF_SRC_DIR)
    subprocess.check_call('/etc/init.d/postgresql-9.3 start')


@worker.task()
def snap_all_tables():
    group(snap.s(*i) for i in tables.list).delay()


@worker.task()
def complete_snapshot(backup_path, db):
    chain(base_backup.si(backup_path, db),
            snap_all_tables.si()).delay()