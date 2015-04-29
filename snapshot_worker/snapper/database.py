__author__ = 'govindchintapalli'

import psycopg2
from celery import Task
import subprocess
from abc import ABCMeta, abstractmethod


def autoretry(func):
    def inner(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            args[0].retry(exc=exc)

    return inner


class AbstractConnectionHandler(object):
    __metaclass__ = ABCMeta

    def __init__(self, db_config={}):
        self.config = db_config
        self._connections = {}

    def connect(self, db_name):
        if db_name not in self._connections:
            self._connections[db_name] = self.new_connection(self.config[db_name])
        return self._connections[db_name]

    def close(self):
        for connection in self._connections.itervalues():
            connection.commit()
            connection.close()

    @abstractmethod
    def new_connection(self, db):
        pass


class PGConnectionHandler(AbstractConnectionHandler):
    PG_CONNECT_STRING = "dbname={NAME} user={USER} password={PASSWORD} host={HOST} port={PORT}"

    def new_connection(self, db):
        return psycopg2.connect(PGConnectionHandler.PG_CONNECT_STRING.format(**db))


class ConnectionHandler(AbstractConnectionHandler):
    def __init__(self, *args, **kwargs):
        super(ConnectionHandler, self).__init__(*args, **kwargs)
        self._specific_handlers = {'postgres': PGConnectionHandler(db_config=self.config)}

    def new_connection(self, db):
        type_ = db['TYPE']
        if type_ in self._specific_handlers:
            return self._specific_handlers[type_].new_connection(db)
        else:
            raise ValueError('cannot handle db type: {}'.format(type_))


class DatabaseConnectionTask(Task):
    abstract = True

    def __init__(self, db_config={}):
        self.config = db_config
        self.connection_handler = ConnectionHandler(self.config)

    @autoretry
    def connection(self, db):
        if db not in self._connections:
            self._connections[db] = self.connection_handler.connect(db)
        return self._connections[db]

    @staticmethod
    def clean(path):
        subprocess.call("rm {}".format(path))

    def __del__(self):
        self.connection_handler.close()

    @autoretry
    def crud(self, db, *args, **kwargs):
        self.connection(db).cursor().execute(*args, **kwargs)
        self.connection.commit()

    @autoretry
    def select(self, db, *args, **kwargs):
        cursor = self.connection(db).cursor()
        cursor.execute(*args, **kwargs)
        return cursor.fetchall()

    @abstractmethod
    def run(self, *args, **kwargs):
        pass