from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ValidationError

from airflow_service import AirflowService
from dag_params import DAG_PARAMS


class DagRunRequest(BaseModel):
    """Generic trigger payload. ``conf`` is validated per-DAG via DAG_PARAMS when a model exists."""
    conf: dict[str, Any] = {}
    unpause: bool = True


app = FastAPI(title='Airflow trigger API')
airflow = AirflowService()


def _validate_conf(dag_id: str, conf: dict[str, Any]) -> dict[str, Any]:
    """Validate conf against the DAG's params model; pass through if the DAG has none registered."""
    params_model = DAG_PARAMS.get(dag_id)
    if params_model is None:
        return conf
    try:
        return params_model(**conf).model_dump()
    except ValidationError as error:
        raise HTTPException(status_code=422, detail=error.errors())


@app.get('/dags')
async def list_dags() -> list[str]:
    return airflow.get_dags()


@app.post('/dags/{dag_id}/runs')
async def trigger_dag(dag_id: str, request: DagRunRequest):
    conf = _validate_conf(dag_id, request.conf)
    dag_run = airflow.trigger_dag(dag_id, conf=conf, unpause=request.unpause)
    return {'message': 'dag was successfully triggered', 'dag_run_id': dag_run.get('dag_run_id')}


@app.get('/dags/{dag_id}/runs/last')
async def get_last_dag_run(dag_id: str):
    last_run = airflow.get_last_dag_run(dag_id)
    if last_run is None:
        raise HTTPException(status_code=404, detail=f'No runs found for DAG {dag_id}')
    return last_run
