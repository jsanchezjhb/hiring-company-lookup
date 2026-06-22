"""
db_utils.py — Database connection via databricks-sql-connector.

Uses the warehouse HTTP path (Thrift/JDBC protocol) with the user's
X-Forwarded-Access-Token from Databricks Apps User Authorization.
This is the same protocol Databricks SQL uses, so if the user can
run queries there, this works too.
"""

import os
import streamlit as st
import pandas as pd

_HOST      = os.environ.get("DATABRICKS_HOST",          "").strip()
_HTTP_PATH = os.environ.get("SQL_WAREHOUSE_HTTP_PATH",  "").strip()


def _user_token() -> str:
    """Get the logged-in user's token from the Databricks Apps header."""
    try:
        t = st.context.headers.get("X-Forwarded-Access-Token", "").strip()
        if t:
            return t
    except AttributeError:
        pass
    # Local dev fallback
    return os.environ.get("DATABRICKS_TOKEN", "").strip()


def run_query(sql_text: str) -> pd.DataFrame:
    """Execute SQL via the SQL connector (Thrift protocol)."""
    from databricks import sql

    token = _user_token()
    if not token:
        raise RuntimeError(
            "No user token available.\n"
            "Make sure User Authorization is enabled in Apps → Edit → User Authorization."
        )

    conn = sql.connect(
        server_hostname=_HOST,
        http_path=_HTTP_PATH,
        access_token=token,
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_text)
            rows = cursor.fetchall()
            cols = [d[0] for d in cursor.description] if cursor.description else []
            return pd.DataFrame(rows, columns=cols)
    finally:
        conn.close()
