"""Connection configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class RedshiftConfig:
    host: str
    port: int
    database: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> "RedshiftConfig":
        """Load configuration from environment variables.

        Required env vars:
            REDSHIFT_HOST     — cluster endpoint
            REDSHIFT_PORT     — default 5439
            REDSHIFT_DATABASE — database name
            REDSHIFT_USER     — IAM or database user
            REDSHIFT_PASSWORD — password (use IAM token for production)
        """
        missing = [
            var
            for var in ("REDSHIFT_HOST", "REDSHIFT_DATABASE", "REDSHIFT_USER", "REDSHIFT_PASSWORD")
            if not os.environ.get(var)
        ]
        if missing:
            raise EnvironmentError(
                f"Missing required environment variables: {', '.join(missing)}\n"
                "Set them before running rsops, e.g.:\n"
                "  export REDSHIFT_HOST=my-cluster.xxxx.us-east-1.redshift.amazonaws.com\n"
                "  export REDSHIFT_DATABASE=dev\n"
                "  export REDSHIFT_USER=admin\n"
                "  export REDSHIFT_PASSWORD=secret"
            )
        return cls(
            host=os.environ["REDSHIFT_HOST"],
            port=int(os.environ.get("REDSHIFT_PORT", "5439")),
            database=os.environ["REDSHIFT_DATABASE"],
            user=os.environ["REDSHIFT_USER"],
            password=os.environ["REDSHIFT_PASSWORD"],
        )
