"""Helpers de conexao DuckDB + MinIO para pipelines do lakehouse."""

from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator

import duckdb


@dataclass(frozen=True)
class MinioConfig:
    endpoint: str
    access_key: str
    secret_key: str
    use_ssl: bool
    bucket: str


def minio_config_from_env() -> MinioConfig:
    """Carrega configuracoes MinIO/Lakehouse das variaveis de ambiente."""
    return MinioConfig(
        endpoint=os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "admin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "admin123"),
        use_ssl=os.getenv("MINIO_USE_SSL", "false").lower() == "true",
        bucket=os.getenv("LAKEHOUSE_BUCKET", "lakehouse-prod"),
    )


def _sql_quote(value: str) -> str:
    return value.replace("'", "''")


def configure_duckdb_for_minio(con: duckdb.DuckDBPyConnection, config: MinioConfig) -> None:
    """Aplica configuracao S3/MinIO em uma conexao DuckDB."""
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{_sql_quote(config.endpoint)}';")
    con.execute(f"SET s3_access_key_id='{_sql_quote(config.access_key)}';")
    con.execute(f"SET s3_secret_access_key='{_sql_quote(config.secret_key)}';")
    con.execute(f"SET s3_use_ssl={'true' if config.use_ssl else 'false'};")
    con.execute("SET s3_url_style='path';")


@contextmanager
def connect_duckdb_minio(database: str = ":memory:") -> Iterator[duckdb.DuckDBPyConnection]:
    """
    Cria conexao DuckDB pronta para escrever/ler no MinIO via s3://.
    Fecha automaticamente ao sair do contexto.
    """
    config = minio_config_from_env()
    con = duckdb.connect(database=database)
    try:
        configure_duckdb_for_minio(con=con, config=config)
        yield con
    finally:
        con.close()
