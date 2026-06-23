# imports
import logging

import pandas as pd
import requests
from sqlalchemy import create_engine
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

# TODO move from hardcoded values to Variables
BASE_URL = 'https://downloads.thebiogrid.org/Download/BioGRID/Release-Archive/BIOGRID-{version}/BIOGRID-ALL-{version}.tab3.zip'
# TODO hardcoded secrets
DATABASE_URL = 'postgresql://postgres:123456@local-db:5432/postgres'


def load_data():
    # todo add logic to check the current version in the db
    # todo move to parameters
    version = '4.4.200'
    logging.info('Loading biogrid file...')
    response = requests.get(
        BASE_URL.format(version=version),
        params={'downloadformat': 'zip'}
    )

    # TODO store in S3
    # PVC or PCV -> mount
    if response.status_code == 200:
        local_file_name = 'biogrid.tab3.zip'
        with open(local_file_name, 'wb') as f:
            f.write(response.content)
    else:
        logging.error('The specified version is not found')
        return

    logging.info('Biogrid file has loaded')
    logging.info('Starting biogrid processing')


def ingest_data():
    # TODO change to XComs
    local_file_name = 'biogrid.tab3.zip'
    version = '4.4.200'
    df = pd.read_csv(local_file_name, delimiter='\t', compression='zip')

    df = df.rename(
        lambda column_name: column_name.lower().replace(' ', '_').replace('#', '_').strip('_'),
        axis='columns'
    )

    df = df[[
        'biogrid_interaction_id',
        'biogrid_id_interactor_a',
        'biogrid_id_interactor_b',
    ]]

    df['version'] = version
     # TODO get rid of index
    logging.info('Biogrid file has been transformed')
    logging.info('Starting ingestion into database...')

    engine = create_engine(DATABASE_URL)
    df.to_sql('biogrid_data', engine, schema='silver', if_exists='replace')
    logging.info('Data successfully ingested')


# dag definition # task API # classical
with DAG(
    dag_id='biogrid_loading_dag',
    schedule=None,
    start_date=None,
    catchup=False,
    tags=['biogrid', 'de_school', 'P1'],
) as dag:
    # tasks
    # TODO add some EmptyOperators: start_op, finish_op
    # TODO set up notifications
    load_data_op = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    ingest_data_op = PythonOperator(
        task_id='ingest_data',
        python_callable=ingest_data,
    )

    # task dependencies
    load_data_op >> ingest_data_op
