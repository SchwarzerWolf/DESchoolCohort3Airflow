from __future__ import annotations

import os

import requests

try:
    from dotenv import load_dotenv

    load_dotenv()
except ModuleNotFoundError:
    pass

AIRFLOW_HOST = os.environ.get('AIRFLOW_BASE_URL', 'http://localhost:8080')
AIRFLOW_USERNAME = os.environ.get('AIRFLOW_USERNAME', 'admin')
AIRFLOW_PASSWORD = os.environ['AIRFLOW_PASSWORD']


class AirflowService:
    """A service to work with the Airflow 3 REST API (/api/v2, JWT auth via SimpleAuthManager)."""

    def __init__(
        self,
        host: str = AIRFLOW_HOST,
        username: str = AIRFLOW_USERNAME,
        password: str = AIRFLOW_PASSWORD,
    ):
        self._host = host.rstrip('/')
        self._username = username
        self._password = password
        self._session = requests.Session()

    def _authenticate(self) -> None:
        """Exchange username/password for a JWT and store it on the session."""
        response = self._session.post(
            f'{self._host}/auth/token',
            json={'username': self._username, 'password': self._password},
            timeout=30,
        )
        response.raise_for_status()
        self._session.headers['Authorization'] = f'Bearer {response.json()["access_token"]}'

    def _request(self, method: str, path: str, **kwargs) -> dict:
        if 'Authorization' not in self._session.headers:
            self._authenticate()

        url = f'{self._host}/api/v2{path}'
        resp = self._session.request(method, url, timeout=30, **kwargs)
        if resp.status_code == 401:
            self._authenticate()
            resp = self._session.request(method, url, timeout=30, **kwargs)

        if not resp.ok:
            raise requests.HTTPError(
                f'{resp.status_code} {resp.reason} for {url}: {resp.text}',
                response=resp,
            )
        return resp.json() if resp.content else {}

    def get_dags(self) -> list[str]:
        dags = self._request('GET', '/dags')['dags']
        return [dag['dag_id'] for dag in dags]

    def trigger_dag(self, dag_id: str, conf: dict | None = None, *, unpause: bool = True) -> dict:
        """Trigger a DAG run. The DAG is created paused, so unpause it first by default."""
        if unpause:
            self._request(
                'PATCH',
                f'/dags/{dag_id}',
                params={'update_mask': 'is_paused'},
                json={'is_paused': False},
            )
        return self._request(
            'POST',
            f'/dags/{dag_id}/dagRuns',
            json={'logical_date': None, 'conf': conf or {}},
        )

    def get_last_dag_run(self, dag_id: str) -> dict | None:
        """Return the most recent run of a DAG (by run_after), or None if it has never run."""
        runs = self._request(
            'GET',
            f'/dags/{dag_id}/dagRuns',
            params={'order_by': '-run_after', 'limit': 1},
        )['dag_runs']
        return runs[0] if runs else None


if __name__ == '__main__':
    airflow = AirflowService()
    print(airflow.get_dags())
