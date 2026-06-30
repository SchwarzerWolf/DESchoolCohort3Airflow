"""Bronze/silver ingestion (raw zip stored in and read back from the S3 bronze bucket)."""
import http
import io
import logging

import pandas as pd
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Variable

from .constants import (
    BRONZE_BUCKET,
    DWH_CONN_ID,
    S3_CONN_ID,
    S3_KEY_TEMPLATE,
    SILVER_SCHEMA,
    SILVER_TABLE,
    WEBSITE_URL_VARIABLE,
)
from .version import get_resolved_version
from ..utils.dataframe import normalize_columns
from ..utils.s3 import download_object, upload_bytes


def build_s3_key(version):
    return S3_KEY_TEMPLATE.format(version=version)


def read_bronze_object(ti):
    """Download the raw zip uploaded by ``bronze_ingestion`` and return (s3_key, raw_bytes)."""
    s3_key = ti.xcom_pull(task_ids='bronze_ingestion', key='s3_key')
    raw_bytes = download_object(s3_key, BRONZE_BUCKET, S3_CONN_ID)
    return s3_key, raw_bytes


def check_existing_version(ti):
    version = get_resolved_version(ti)
    postgres_hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    connection = postgres_hook.get_conn()

    try:
        with connection.cursor() as cursor:
            cursor.execute(f'SELECT DISTINCT(bd."version") FROM {SILVER_SCHEMA}.{SILVER_TABLE} bd;')
            loaded_versions = [row[0] for row in cursor.fetchall()]
    except Exception as error:
        # The silver table does not exist yet on the very first run.
        logging.warning(f'Could not read loaded versions (table may not exist yet): {error}')
        connection.rollback()
        loaded_versions = []
    if version not in loaded_versions:
        return 'bronze_ingestion'
    else:
        return 'finish'


def bronze_ingestion(ti):
    version = get_resolved_version(ti)
    url = Variable.get(WEBSITE_URL_VARIABLE)
    if not url:
        raise Exception('Biogrid website url is not set, please set it in Airflow Variables')

    logging.info('Loading biogrid file...')
    response = requests.get(
        url.format(version=version),
        params={'downloadformat': 'zip'}
    )

    if response.status_code != http.HTTPStatus.OK:
        logging.error('The specified version is not found')
        raise Exception('The specified version is not found')

    s3_key = build_s3_key(version)
    upload_bytes(response.content, s3_key, BRONZE_BUCKET, S3_CONN_ID, replace=True)
    ti.xcom_push(key='s3_key', value=s3_key)

    logging.info(f'Biogrid file uploaded to s3://{BRONZE_BUCKET}/{s3_key}')
    logging.info('Starting biogrid processing')


def silver_ingestion(ti):
    _, raw_bytes = read_bronze_object(ti)
    version = get_resolved_version(ti)
    df = pd.read_csv(io.BytesIO(raw_bytes), delimiter='\t', compression='zip')

    df = normalize_columns(df)

    df = df[[
        'biogrid_interaction_id',
        'biogrid_id_interactor_a',
        'biogrid_id_interactor_b',
    ]]

    df['version'] = version
    logging.info('Biogrid file has been transformed')
    logging.info('Starting ingestion into database...')

    postgres_hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    engine = postgres_hook.get_sqlalchemy_engine()

    df.to_sql(
        SILVER_TABLE,
        engine,
        index=False,
        schema=SILVER_SCHEMA,
        if_exists='append',
    )
    logging.info('Data successfully ingested')
