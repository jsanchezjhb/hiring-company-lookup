"""
db_utils.py — Connects using the logged-in user's token via User Authorization.

The user's OAuth token (X-Forwarded-Access-Token) is injected by Databricks
Apps when User Authorization is enabled. Queries run as that user, inheriting
their personal Unity Catalog permissions — no service principal grants needed.
"""

import os
import streamlit as st
import pandas as pd

_HOST      = os.environ.get("DATABRICKS_HOST",         "").strip()
_HTTP_PATH = os.environ.get("SQL_WAREHOUSE_HTTP_PATH", "").strip()


def _get_token() -> str:
    try:
        t = st.context.headers.get("X-Forwarded-Access-Token", "").strip()
        if t:
            return t
    except AttributeError:
        pass
    return os.environ.get("DATABRICKS_TOKEN", "").strip()


def run_query(sql_text: str) -> pd.DataFrame:
    from databricks import sql as dbsql

    token = _get_token()
    if not token:
        raise RuntimeError(
            "No user token found. "
            "Enable User Authorization in Apps → Edit → User Authorization."
        )

    # credentials_provider passes the OAuth JWT as a Bearer header.
    # This is the correct approach for OAuth tokens — access_token only
    # works for PAT tokens (dapi...), not OAuth JWTs (eyJ...).
    def _creds():
        return {"Authorization": f"Bearer {token}"}

    conn = dbsql.connect(
        server_hostname=_HOST,
        http_path=_HTTP_PATH,
        credentials_provider=_creds,
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_text)
            rows = cursor.fetchall()
            cols = [d[0] for d in cursor.description] if cursor.description else []
            return pd.DataFrame(rows, columns=cols)
    finally:
        conn.close()
