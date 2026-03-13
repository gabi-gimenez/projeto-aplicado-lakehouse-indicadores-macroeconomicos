import json
import os
import tempfile
from datetime import datetime, timedelta, timezone

import requests
from airflow.decorators import dag, task
from include.plugins.minio_duckdb import connect_duckdb_minio, minio_config_from_env
from pendulum import datetime as pdatetime
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError as RequestsConnectionError
from urllib3.util.retry import Retry

SOURCE_SYSTEM = "shopnex_fast_store_api"
API_BASE_URLS = tuple(
    url.strip().rstrip("/")
    for url in os.getenv(
        "SHOPNEX_API_BASE_URLS",
        "http://host.docker.internal:8000,http://172.17.0.1:8000",
    ).split(",")
    if url.strip()
)
TARGET_BUCKET = minio_config_from_env().bucket
ENDPOINTS = ("users", "carts", "products")

def _sql_quote(value: str) -> str:
    return value.replace("'", "''")


def _build_http_session() -> requests.Session:
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
    )
    adapter = HTTPAdapter(max_retries=retry)

    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _fetch_endpoint_payload(session: requests.Session, endpoint: str) -> list:
    errors: list[str] = []
    for base_url in API_BASE_URLS:
        endpoint_url = f"{base_url}/{endpoint}"
        try:
            response = session.get(endpoint_url, timeout=60)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, list):
                raise ValueError(f"Resposta da API '{endpoint}' deve ser uma lista.")
            return payload
        except RequestsConnectionError as exc:
            errors.append(f"{endpoint_url} -> {exc}")

    raise RuntimeError(
        f"Nao foi possivel conectar ao endpoint '{endpoint}' em nenhuma URL configurada: "
        f"{', '.join(API_BASE_URLS)}. Erros: {' | '.join(errors)}"
    )


@dag(
    dag_id="extract_shopnex_api_to_bronze",
    start_date=pdatetime(2026, 3, 11),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["lakehouse", "bronze", "shopnex", "users", "carts", "products"],
)
def extract_shopnex_api_to_bronze():
    @task
    def ingest_endpoint_to_bronze(endpoint: str) -> str:
        now_utc = datetime.now(timezone.utc)
        ingestion_date = now_utc.date().isoformat()
        ingestion_ts = now_utc.strftime("%Y-%m-%d %H:%M:%S")
        run_ts = now_utc.strftime("%Y%m%dT%H%M%SZ")

        with _build_http_session() as session:
            payload = _fetch_endpoint_payload(session=session, endpoint=endpoint)

        if not payload:
            raise ValueError(f"API '{endpoint}' retornou lista vazia; nada para gravar.")

        output_path = (
            f"s3://{TARGET_BUCKET}/bronze/{SOURCE_SYSTEM}/{endpoint}/"
            f"ingestion_date={ingestion_date}/{endpoint}_{run_ts}.parquet"
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=True) as temp_json:
            json.dump(payload, temp_json, ensure_ascii=True)
            temp_json.flush()

            with connect_duckdb_minio(database=":memory:") as con:
                con.execute(
                    f"""
                    COPY (
                        SELECT
                            *,
                            TIMESTAMP '{_sql_quote(ingestion_ts)}' AS _ingestion_ts_utc,
                            DATE '{_sql_quote(ingestion_date)}' AS _ingestion_date,
                            '{_sql_quote(SOURCE_SYSTEM)}' AS _source_system,
                            '{_sql_quote(endpoint)}' AS _source_endpoint
                        FROM read_json_auto('{_sql_quote(temp_json.name)}')
                    )
                    TO '{_sql_quote(output_path)}'
                    (FORMAT PARQUET, COMPRESSION ZSTD);
                    """
                )

        return output_path

    for endpoint in ENDPOINTS:
        ingest_endpoint_to_bronze.override(task_id=f"ingest_{endpoint}_to_bronze")(endpoint=endpoint)


extract_shopnex_api_to_bronze()