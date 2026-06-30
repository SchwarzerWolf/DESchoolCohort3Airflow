"""Data-quality gates (Pandera on bronze, Soda on silver & gold)."""
import io
import logging
import zipfile

import pandas as pd

from .constants import (
    BRONZE_BUCKET,
    GOLD_CHECKS_FILE,
    GOLD_DATA_SOURCE,
    S3_CONN_ID,
    SILVER_CHECKS_FILE,
    SILVER_DATA_SOURCE,
)
from .ingestion import read_bronze_object
from .version import get_resolved_version
from ..utils.dataframe import normalize_columns
from ..utils.s3 import object_exists
from ..utils.soda import run_soda_scan


def bronze_quality_checks(ti):
    """Validate the raw artifact in object storage before promoting it to silver.

    Two levels: structural (file-level) checks with plain Python, then schema-level validation of the
    extracted table with Pandera.
    """
    import pandera.pandas as pa

    s3_key, raw_bytes = read_bronze_object(ti)

    # Structural / file-level checks
    if not object_exists(s3_key, BRONZE_BUCKET, S3_CONN_ID):
        raise Exception(f'Bronze object not found: s3://{BRONZE_BUCKET}/{s3_key}')
    if len(raw_bytes) == 0:
        raise Exception('Bronze object is empty')
    buffer = io.BytesIO(raw_bytes)
    if not zipfile.is_zipfile(buffer):
        raise Exception('Bronze object is not a valid zip file')
    buffer.seek(0)

    # Schema-level checks (Pandera) on the extracted table
    df = normalize_columns(pd.read_csv(buffer, delimiter='\t', compression='zip'))
    bronze_schema = pa.DataFrameSchema(
        {
            'biogrid_interaction_id': pa.Column(int, nullable=False, unique=True, coerce=True),
            'biogrid_id_interactor_a': pa.Column(int, nullable=False, coerce=True),
            'biogrid_id_interactor_b': pa.Column(int, nullable=False, coerce=True),
        },
        strict=False,
    )
    bronze_schema.validate(df, lazy=True)
    logging.info('Bronze data quality checks passed')


def silver_quality_checks(ti):
    version = get_resolved_version(ti)
    run_soda_scan(SILVER_DATA_SOURCE, SILVER_CHECKS_FILE, variables={'version': version})


def gold_quality_checks():
    run_soda_scan(GOLD_DATA_SOURCE, GOLD_CHECKS_FILE)
