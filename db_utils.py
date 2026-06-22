"""
db_utils.py — Database connection via direct REST API calls.

Bypasses the Databricks SDK's Config validation entirely, which was causing
"two auth methods" conflicts between DATABRICKS_TOKEN and OAuth credentials.

Auth priority:
  1. X-Forwarded-Access-Token header  — user's token (User Authorization)
  2. DATABRICKS_TOKEN env var         — PAT for local dev
  3. OAuth M2M via OIDC endpoint      — service principal (production fallback)
"""

import os
import requests
import streamlit as st
import pandas as pd

_HOST = os.environ.get("DATABRICKS_HOST", "").strip()


def _get_token() -> str:
    """Return the best available auth token."""

    # 1. User Authorization (Databricks Apps injects this per-user)
    try:
        token = st.context.headers.get("X-Forwarded-Access-Token", "")
        if token:
            return token
    except AttributeError:
        pass

    # 2. PAT for local dev
    pat = os.environ.get("DATABRICKS_TOKEN", "").strip()
    if pat:
        return pat

    # 3. OAuth M2M — fetch token from OIDC endpoint
    client_id     = os.environ.get("DATABRICKS_CLIENT_ID", "").strip()
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
    path = os.environ.get("SQL_WAREHOUSE_HTTP_PATH", "").rstrip("/")
    wid  = path.split("/")[-1]
    if not wid or wid == "YOUR_WAREHOUSE_ID_HERE":
        raise RuntimeError(
            "SQL_WAREHOUSE_HTTP_PATH is not configured.\n"
            "Set it to the HTTP Path from your SQL Warehouse Connection Details."
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
            "statement":   sql_text,
            "warehouse_id": wid,
            "wait_timeout": "50s",
            "disposition":  "INLINE",
            "format":       "JSON_ARRAY",
        },
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()

    state = data.get("status", {}).get("state", "UNKNOWN")
    if state != "SUCCEEDED":
        error = data.get("status", {}).get("error", {})
        raise Exception(f"Query failed [{state}]: {error.get('message', state)}")

    result = data.get("result", {})
    if not result or not result.get("data_array"):
        return pd.DataFrame()

    columns = [col["name"] for col in data["manifest"]["schema"]["columns"]]
    return pd.DataFrame(result["data_array"], columns=columns)
