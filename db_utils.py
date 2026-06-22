"""
db_utils.py — Database connection via databricks-sql-connector.

Uses credentials_provider to pass the user's OAuth JWT token
(X-Forwarded-Access-Token) from Databricks Apps User Authorization.
Matches the exact connector + SDK versions from the working internal app.
"""

import os
import streamlit as st
import pandas as pd

_HOST      = os.environ.get("DATABRICKS_HOST",         "").strip()
_HTTP_PATH = os.environ.get("SQL_WAREHOUSE_HTTP_PATH", "").strip()


def _user_token() -> str:
    """Get the logged-in user's OAuth token from Databricks Apps."""
    try:
        t = st.context.headers.get("X-Forwarded-Access-Token", "").strip()
        if t:
            return t
    except AttributeError:
        pass
    # Local dev fallback
    return os.environ.get("DATABRICKS_TOKEN", "").strip()


def run_query(sql_text: str) -> pd.DataFrame:
    """Execute SQL via the SQL connector using the user's OAuth token."""
    from databricks import sql

    token = _user_token()
    if not token:
        raise RuntimeError(
            "No user token available. "
            "Ensure User Authorization is enabled in Apps → Edit → User Authorization."
        )

    # credentials_provider is the correct way to pass an OAuth JWT token.
    # access_token only works for PAT (dapi...) tokens.
    def token_provider():
        return {"Authorization": f"Bearer {token}"}

    conn = sql.connect(
        server_hostname=_HOST,
        http_path=_HTTP_PATH,
        credentials_provider=token_provider,
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_text)
            rows = cursor.fetchall()
            cols = [d[0] for d in cursor.description] if cursor.description else []
            return pd.DataFrame(rows, columns=cols)
    finally:
        conn.close()
