from abc import abstractmethod
import subprocess
import database

__author__ = 'govindchintapalli'


class FetchTableTask(database.DatabaseConnectionTask):
    abstract = True

    @database.autoretry
    def run(self, source_db_config, table, path, *args):
        self.fetch_table(source_db_config, table, path)

    @abstractmethod
    def fetch_table(self, source_db_config, table, path):
        pass


class LoadTableTask(database.DatabaseConnectionTask):
    abstract = True

    @database.autoretry
    def run(self, target_db_config, file_path, *args):
        print target_db_config
        print file_path
        print args
        self.load_table(target_db_config, file_path)

    @abstractmethod
    def load_table(self, target_db_config, file_path):
        pass


class PGFetchTableTask(FetchTableTask):
    FETCH_COMMAND = 'PGPASSWORD="{PASSWORD}" /usr/bin/pg_dump -U {USER} -h {HOST} -p {PORT} {NAME} -t {0} > {1}\n'

    def fetch_table(self, source_db_config, table, path):
        subprocess.check_call(self.FETCH_COMMAND.format(table, path, **source_db_config), shell=True)


class PGLoadTableTask(LoadTableTask):
    LOAD_COMMAND = 'PGPASSWORD="{PASSWORD}" psql -U {USER} -h {HOST} -p {PORT} {NAME} -f {0}\n'

    def load_table(self, target_db_config, file_path):
        subprocess.check_call(self.LOAD_COMMAND.format(file_path, **target_db_config), shell=True)