"""
db_utils.py — Database connection via direct REST API.

Tries every known location where Databricks Apps can inject the user's token,
so queries run under the logged-in user's identity and inherit their permissions.
"""

import os
import requests
import streamlit as st
import pandas as pd

_HOST = os.environ.get("DATABRICKS_HOST", "").strip()


def _get_token() -> str:
    """
    Return the best available token, trying all known injection points.

    Databricks Apps with User Authorization enabled may inject the user's
    token in several ways depending on platform version — we try them all.
    """
    # 1. X-Forwarded-Access-Token header (Databricks Apps standard)
    try:
        t = st.context.headers.get("X-Forwarded-Access-Token", "").strip()
        if t:
            return t
    except AttributeError:
        pass

    # 2. Authorization header (Bearer <token>)
    try:
        auth = st.context.headers.get("Authorization", "").strip()
        if auth.lower().startswith("bearer "):
            t = auth[7:].strip()
            if t:
                return t
    except AttributeError:
        pass

    # 3. DATABRICKS_TOKEN env var (may be set per-session by User Authorization)
    t = os.environ.get("DATABRICKS_TOKEN", "").strip()
    if t:
        return t

    # 4. OAuth M2M — service principal (may 403 if SP lacks workspace access)
    client_id     = os.environ.get("DATABRICKS_CLIENT_ID",     "").strip()
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET", "").strip()

    resp = requests.post(
        f"https://{_HOST}/oidc/v1/token",
        auth=(client_id, client_secret),
        data={"grant_type": "client_credentials", "scope": "all-apis"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _warehouse_id() -> str:
    """
    Extract warehouse ID from SQL_WAREHOUSE_HTTP_PATH.
    e.g. /sql/1.0/warehouses/abc123  →  abc123
    """
    path = os.environ.get("SQL_WAREHOUSE_HTTP_PATH", "").rstrip("/")
    wid  = path.split("/")[-1]
    if not wid or wid == "YOUR_WAREHOUSE_ID_HERE":
        raise RuntimeError(
            "SQL_WAREHOUSE_HTTP_PATH is not configured.\n"
            "Set it to the HTTP Path from: SQL Warehouses → [warehouse] → Connection Details."
        )
    return wid


def run_query(sql_text: str) -> pd.DataFrame:
    """Execute SQL via the Databricks Statement Execution REST API."""
    token = _get_token()
    wid   = _warehouse_id()

    resp = requests.post(
        f"https://{_HOST}/api/2.0/sql/statements",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "statement":    sql_text,
            "warehouse_id": wid,
            "wait_timeout": "50s",
            "disposition":  "INLINE",
            "format":       "JSON_ARRAY",
        },
        timeout=60,
    )
    resp.raise_for_status()
    data  = resp.json()
    state = data.get("status", {}).get("state", "UNKNOWN")

    if state != "SUCCEEDED":
        error = data.get("status", {}).get("error", {})
        raise Exception(f"Query failed [{state}]: {error.get('message', state)}")

    result = data.get("result", {})
    if not result or not result.get("data_array"):
        return pd.DataFrame()

    columns = [col["name"] for col in data["manifest"]["schema"]["columns"]]
    return pd.DataFrame(result["data_array"], columns=columns)
