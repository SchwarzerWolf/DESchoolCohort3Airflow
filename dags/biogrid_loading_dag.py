import http
import logging
from datetime import timedelta

import pandas as pd
import requests
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.sdk import DAG, Param, Variable
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator

def send_teams_alert(context):
    webhook_url = 'https://add-your-token'
    dag_id = context['task_instance'].dag_id
    task_id = context['task_instance'].task_id

    message = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "TextBlock",
                            "text": "🚨 **Airflow Task Failed!**",
                            "wrap": True,
                            "weight": "Bolder",
                            "color": "Attention",
                            "size": "Medium"
                        },
                        {
                            "type": "FactSet",
                            "facts": [
                                {"title": "DAG", "value": dag_id},
                                {"title": "Task", "value": task_id},
                            ]
                        }
                    ]
                }
            }
        ]
    }
    response = requests.post(webhook_url, json=message)

    if response.status_code == http.HTTPMethod.OK:
        logging.info('Alert send successfully')
    else:
        logging.error(f'Failed to send message to Teams: {response.status_code} {response.text}')


def check_existing_version(params):
    version = params['version']
    postgres_hook = PostgresHook(postgres_conn_id='dwh_connection')
    connection = postgres_hook.get_conn()

    with connection.cursor() as cursor:
        cursor.execute('SELECT DISTINCT(bd."version") FROM silver.biogrid_data bd;')
        loaded_versions = [version[0] for version in cursor.fetchall()]
        if version not in loaded_versions:
            return 'bronze_ingestion'
        else:
            return 'finish'

def bronze_ingestion(params, ti):
    version = params['version']
    url = Variable.get('biogrid_website_url')
    if not url:
        raise Exception('Biogrid website url is not set, please set it in Airflow Variables')

    logging.info('Loading biogrid file...')
    response = requests.get(
        url.format(version=version),
        params={'downloadformat': 'zip'}
    )

    # TODO store in S3
    if response.status_code == http.HTTPStatus.OK:
        local_file_name = f'biogrid_{version}.tab3.zip'
        ti.xcom_push(key='local_file_name', value=local_file_name)
        with open(local_file_name, 'wb') as f:
            f.write(response.content)
    else:
        logging.error('The specified version is not found')
        raise Exception('The specified version is not found')

    logging.info('Biogrid file has loaded')
    logging.info('Starting biogrid processing')


def silver_ingestion(params, ti):
    local_file_name = ti.xcom_pull(
        task_ids='bronze_ingestion',
        key='local_file_name',
    )
    version = params['version']
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
    logging.info('Biogrid file has been transformed')
    logging.info('Starting ingestion into database...')

    postgres_hook = PostgresHook(postgres_conn_id='dwh_connection')
    engine = postgres_hook.get_sqlalchemy_engine()

    df.to_sql(
        'biogrid_data',
        engine,
        index=False,
        schema='silver',
        if_exists='append',
    )
    logging.info('Data successfully ingested')


with DAG(
    dag_id='biogrid_loading_dag',
    schedule=None,
    start_date=None,
    catchup=False,
    tags=['biogrid', 'de_school', 'P1'],
    params={
        'version': Param(type='string'),
    },
    dagrun_timeout=timedelta(minutes=30),
    default_args={
        'owner': 'data-platform',
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
        'retry_exponential_backoff': True,
        'max_retry_delay': timedelta(minutes=30),
        'on_failure_callback': send_teams_alert,
    },
) as dag:
    start_op = EmptyOperator(
        task_id='start',
    )

    check_existing_version_op = BranchPythonOperator(
        task_id = 'check_existing_version',
        python_callable=check_existing_version,
    )

    bronze_ingestion_op = PythonOperator(
        task_id='bronze_ingestion',
        python_callable=bronze_ingestion,
    )

    silver_ingestion_op = PythonOperator(
        task_id='silver_ingestion',
        python_callable=silver_ingestion,
    )

    gold_ingestion_op = SQLExecuteQueryOperator(
        task_id='gold_ingestion',
        sql='SELECT gold.get_biogrid_interactors();',
        conn_id='dwh_connection',
    )

    finish_op = EmptyOperator(
        task_id='finish',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    start_op >> check_existing_version_op >> bronze_ingestion_op >> silver_ingestion_op >> gold_ingestion_op >> finish_op
    check_existing_version_op >> finish_op
