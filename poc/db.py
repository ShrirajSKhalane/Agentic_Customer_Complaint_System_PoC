from __future__ import annotations

import json
import os
from contextlib import contextmanager
from typing import Any, Iterator
from uuid import UUID

import psycopg
from psycopg.rows import dict_row


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://complaints:complaints@localhost:5432/complaints",
)
CONNECT_TIMEOUT = int(os.getenv("PGCONNECT_TIMEOUT", "3"))


def connect(
    *, row_factory: Any = dict_row
) -> psycopg.Connection[Any]:
    return psycopg.connect(
        DATABASE_URL,
        row_factory=row_factory,
        connect_timeout=CONNECT_TIMEOUT,
    )


@contextmanager
def connection() -> Iterator[psycopg.Connection[dict[str, Any]]]:
    with connect() as conn:
        yield conn


def claim_event(
    conn: psycopg.Connection[Any], consumer_name: str, event_id: UUID
) -> bool:
    row = conn.execute(
        """
        INSERT INTO processed_events (consumer_name, event_id)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING
        RETURNING event_id
        """,
        (consumer_name, event_id),
    ).fetchone()
    return row is not None


def json_safe(value: Any) -> Any:
    """Postgres JSONB rejects NUL bytes that live models sometimes emit."""
    if isinstance(value, str):
        return value.replace("\x00", "")
    if isinstance(value, dict):
        return {json_safe(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [json_safe(item) for item in value]
    return value


def write_trace(
    correlation_id: str,
    agent: str,
    action: str,
    status: str = "COMPLETED",
    detail: dict[str, Any] | None = None,
    *,
    conn: psycopg.Connection[Any] | None = None,
) -> None:
    owns_connection = conn is None
    target = conn or connect()
    try:
        target.execute(
            """
            INSERT INTO event_trace
                (correlation_id, agent, action, status, detail)
            VALUES (%s, %s, %s, %s, %s::jsonb)
            """,
            (
                correlation_id,
                agent,
                action,
                status,
                json.dumps(json_safe(detail or {})),
            ),
        )
        if owns_connection:
            target.commit()
    finally:
        if owns_connection:
            target.close()


def fetch_all(sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with connection() as conn:
        return list(conn.execute(sql, params).fetchall())


def fetch_one(
    sql: str, params: tuple[Any, ...] = ()
) -> dict[str, Any] | None:
    with connection() as conn:
        return conn.execute(sql, params).fetchone()
