"""Per-DAG parameter models used to validate the trigger `conf`.

To support a new DAG with validated params:
  1. add a model subclassing ``DagParams`` (extra keys are rejected);
  2. register it in ``DAG_PARAMS`` under the DAG id.
DAGs without an entry accept arbitrary conf (no validation).
"""
from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class DagParams(BaseModel):
    """Base for all DAG params: forbid unknown fields so bad params are caught."""
    model_config = ConfigDict(extra='forbid')


class BiogridParams(DagParams):
    version: str = 'latest'


# dag_id -> params model
DAG_PARAMS: dict[str, type[DagParams]] = {
    'biogrid_loading_dag': BiogridParams,
}
