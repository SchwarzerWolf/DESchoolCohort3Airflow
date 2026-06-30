"""BioGRID version resolution (default ``latest`` resolved from the website)."""
import logging
import re

import requests
from airflow.sdk import Variable

from .constants import (
    DEFAULT_RELEASES_URL,
    LATEST_VERSION,
    RELEASES_URL_VARIABLE,
    VERSION_PATTERN,
)


def resolve_version(params):
    """Resolve the BioGRID version to load.

    When the ``version`` param is the default ``latest`` sentinel, scrape the BioGRID release
    archive and return the newest released version. Otherwise pass the user-supplied value through.
    """
    version = params['version']
    if version and version != LATEST_VERSION:
        logging.info(f'Using user-supplied BioGRID version: {version}')
        return version

    releases_url = Variable.get(RELEASES_URL_VARIABLE, DEFAULT_RELEASES_URL)
    logging.info(f'Resolving the latest BioGRID version from {releases_url}')
    response = requests.get(releases_url, timeout=60)
    response.raise_for_status()

    versions = re.findall(VERSION_PATTERN, response.text)
    if not versions:
        raise Exception('Could not find any BioGRID versions at the releases URL')

    latest = max(set(versions), key=lambda v: tuple(int(part) for part in v.split('.')))
    logging.info(f'Resolved the latest BioGRID version: {latest}')
    return latest


def get_resolved_version(ti):
    """The BioGRID version produced by the ``resolve_version`` task."""
    version = ti.xcom_pull(task_ids='resolve_version')
    if not version:
        raise Exception('Resolved version is not available from the resolve_version task')
    return version
