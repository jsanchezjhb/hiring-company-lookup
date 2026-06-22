"""
db_utils.py — Exact connection pattern from the working billing-disputes app.

Uses databricks.sdk.core.Config with credentials_provider=lambda: cfg.authenticate
to let the SDK handle OAuth M2M auth via DATABRICKS_CLIENT_ID + CLIENT_SECRET.

User Authorization must be OFF — the SDK Config handles auth on its own.
"""

import os
import pandas as pd

DATABRICKS_HTTP_PATH = os.environ.get("SQL_WAREHOUSE_HTTP_PATH", "/sql/1.0/warehouses/16984dfe9a2c3705").strip()


def get_conn():
    from databricks.sdk.core import Config
    from databricks import sql
    cfg = Config()
    return sql.connect(
        server_hostname=cfg.host,
        http_path=DATABRICKS_HTTP_PATH,
        credentials_provider=lambda: cfg.authenticate,
    )


def run_query(sql_text: str) -> pd.DataFrame:
    try:
        with get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_text)
                rows = cursor.fetchall()
                cols = [d[0] for d in cursor.description] if cursor.description else []
                return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        raise Exception(str(e)) from e
