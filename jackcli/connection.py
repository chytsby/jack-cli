"""Redshift connection layer with retry logic."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

import psycopg2
import psycopg2.extensions
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from .config import RedshiftConfig


class ConnectionError(Exception):
    """Raised when a Redshift connection cannot be established."""


class PermissionError(Exception):
    """Raised when the user lacks sufficient privileges for an operation."""


def _is_retryable(exc: BaseException) -> bool:
    """Return True for transient network / timeout errors, False for auth/permission errors."""
    if not isinstance(exc, psycopg2.Error):
        return False
    # psycopg2 uses SQLSTATE codes; 08xxx = connection exceptions
    sqlstate = getattr(exc, "pgcode", "") or ""
    if sqlstate.startswith("08"):
        return True
    msg = str(exc).lower()
    return any(kw in msg for kw in ("timeout", "connection refused", "could not connect"))


@retry(
    retry=retry_if_exception_type(psycopg2.OperationalError),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    reraise=True,
)
def _connect(cfg: RedshiftConfig) -> psycopg2.extensions.connection:
    try:
        return psycopg2.connect(
            host=cfg.host,
            port=cfg.port,
            dbname=cfg.database,
            user=cfg.user,
            password=cfg.password,
            connect_timeout=10,
            sslmode="require",
        )
    except psycopg2.OperationalError as exc:
        if _is_retryable(exc):
            raise  # tenacity will retry
        raise ConnectionError(f"Cannot connect to Redshift: {exc}") from exc


@contextmanager
def get_connection(cfg: RedshiftConfig) -> Generator[psycopg2.extensions.connection, None, None]:
    """Context manager that yields a connection and closes it on exit."""
    conn: psycopg2.extensions.connection | None = None
    try:
        conn = _connect(cfg)
        yield conn
    except psycopg2.ProgrammingError as exc:
        # Typically a permission / syntax error — not retryable
        raise PermissionError(f"Permission or SQL error: {exc}") from exc
    finally:
        if conn and not conn.closed:
            conn.close()
