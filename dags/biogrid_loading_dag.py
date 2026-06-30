from datetime import timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.sdk import DAG, Param
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator

from lib.biogrid.scripts import GOLD_INGESTION_SCRIPT
from lib.biogrid.constants import DWH_CONN_ID, LATEST_VERSION
from lib.biogrid.ingestion import (
    bronze_ingestion,
    check_existing_version,
    silver_ingestion,
)
from lib.biogrid.quality import (
    bronze_quality_checks,
    gold_quality_checks,
    silver_quality_checks,
)
from lib.biogrid.version import resolve_version
from lib.utils.teams import send_teams_alert

with DAG(
    dag_id='biogrid_loading_dag',
    schedule='@monthly',
    start_date=None,
    catchup=False,
    tags=['biogrid', 'de_school', 'P1'],
    params={
        'version': Param(default=LATEST_VERSION, type='string'),
    },
    dagrun_timeout=timedelta(minutes=30),
    default_args={
        'owner': 'data-platform',
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

    resolve_version_op = PythonOperator(
        task_id='resolve_version',
        python_callable=resolve_version,
    )

    check_existing_version_op = BranchPythonOperator(
        task_id='check_existing_version',
        python_callable=check_existing_version,
        pool='db_calls_pool',
    )

    bronze_ingestion_op = PythonOperator(
        task_id='bronze_ingestion',
        python_callable=bronze_ingestion,
    )

    bronze_quality_checks_op = PythonOperator(
        task_id='bronze_quality_checks',
        python_callable=bronze_quality_checks,
    )

    silver_ingestion_op = PythonOperator(
        task_id='silver_ingestion',
        python_callable=silver_ingestion,
        pool='db_calls_pool',
        pool_slots=2,
    )

    silver_quality_checks_op = PythonOperator(
        task_id='silver_quality_checks',
        python_callable=silver_quality_checks,
        pool='db_calls_pool',
    )

    gold_ingestion_op = SQLExecuteQueryOperator(
        task_id='gold_ingestion',
        sql=GOLD_INGESTION_SCRIPT,
        conn_id=DWH_CONN_ID,
        pool='db_calls_pool',
    )

    gold_quality_checks_op = PythonOperator(
        task_id='gold_quality_checks',
        python_callable=gold_quality_checks,
        pool='db_calls_pool',
    )

    finish_op = EmptyOperator(
        task_id='finish',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    start_op >> resolve_version_op >> check_existing_version_op
    (
        check_existing_version_op
        >> bronze_ingestion_op
        >> bronze_quality_checks_op
        >> silver_ingestion_op
        >> silver_quality_checks_op
        >> gold_ingestion_op
        >> gold_quality_checks_op
        >> finish_op
    )
    check_existing_version_op >> finish_op
