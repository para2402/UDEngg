import os
from configparser import ConfigParser

PROJECT_WORKSPACE = os.environ['PROJECT_WORKSPACE']

AIRFLOW_CONFIG_PATH = '/root/airflow/airflow.cfg'
config = ConfigParser()
config.read(AIRFLOW_CONFIG_PATH)

config['core']['dags_folder'] = PROJECT_WORKSPACE + '/airflow/dags'
config['core']['load_examples'] = 'False'
config['core']['plugins_folder'] = PROJECT_WORKSPACE + '/airflow/plugins'
config['core']['sql_alchemy_conn'] = 'postgresql+psycopg2://airflow:airflow@localhost:5432/airflow'
config['core']['executor'] = 'LocalExecutor'

config['webserver']['expose_config'] = 'True'
config['webserver']['reload_on_plugin_change'] = 'True'

with open(AIRFLOW_CONFIG_PATH, 'w') as fp:
    config.write(fp)

os.system('airflow db init')

######################################################################################

# PROJECT_WORKSPACE
UDACITY_CONFIG_PATH = PROJECT_WORKSPACE + '/config.cfg'
config = ConfigParser()
config.read(UDACITY_CONFIG_PATH)


ROOT_BUCKET = config['S3']['ROOT_BUCKET']
RAW_DATA_KEY = config['S3']['RAW_DATA_KEY']
STAGING_DATA_KEY = config['S3']['STAGING_DATA_KEY']
SPARK_SCRIPTS_KEY = config['S3']['SPARK_SCRIPTS_KEY']
os.system(f'airflow variables set ROOT_BUCKET {ROOT_BUCKET}')
os.system(f'airflow variables set RAW_DATA_KEY {RAW_DATA_KEY}')
os.system(f'airflow variables set STAGING_DATA_KEY {STAGING_DATA_KEY}')
os.system(f'airflow variables set SPARK_SCRIPTS_KEY {SPARK_SCRIPTS_KEY}')

REDSHIFT_S3_ROLE=config['REDSHIFT']['REDSHIFT_S3_ROLE']
os.system(f'airflow variables set REDSHIFT_S3_ROLE {REDSHIFT_S3_ROLE}')

EMR_EC2_KEY_NAME=config['EMR']['EMR_EC2_KEY_NAME']
os.system(f'airflow variables set EMR_EC2_KEY_NAME {EMR_EC2_KEY_NAME}')
