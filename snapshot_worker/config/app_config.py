__author__ = 'govindchintapalli'

APP_NAME = 'table_snapshots'
TEMPORARY_FILE_PATH = '/tmp/'
BASE_BACKUP_PATH = '/var/lib/pgsql/9.3/data/'
ANALYTICS_SERVER = {'USER': 'housing_analytics',
                    'PASSWORD': 'housing',
                    'NAME': 'analytics',
                    'HOST': 'localhost',
                    'PORT': '5432',
                    'TYPE': 'postgres'}

CONF_SRC_DIR = '/var/lib/pgsql/9.3/data/'
CONF_MATCH='*.conf'
CONF_DST_DIR = '/var/lib/pgsql/9.3/bkp/'