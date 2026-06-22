"""
db_utils.py — Database connection for the Fraud Detection App.

With User Authorization enabled in Databricks Apps, each user's requests
carry their OAuth token in the X-Forwarded-Access-Token header.
We read that header and pass it to WorkspaceClient so queries run as the
logged-in user — inheriting their Unity Catalog permissions.

Falls back to environment-based auth (service principal / PAT) for local dev.
"""

import os
import streamlit as st
import pandas as pd


# ---------------------------------------------------------------------------
# Client — per user, not cached globally
# ---------------------------------------------------------------------------

def _sdk_client():
    """
    Return a WorkspaceClient authenticated as the current user.

    Databricks Apps injects the logged-in user's token via the
    X-Forwarded-Access-Token request header when User Authorization is on.
    We pass that token explicitly so Unity Catalog sees the user's identity,
    not the app service principal's.
    """
    from databricks.sdk import WorkspaceClient

    host = os.environ.get("DATABRICKS_HOST", "").strip()

    # Read the per-user token injected by Databricks Apps
    user_token = ""
    try:
        user_token = st.context.headers.get("X-Forwarded-Access-Token", "")
    except AttributeError:
        # st.context.headers requires Streamlit >= 1.37
        pass

    if user_token:
        return WorkspaceClient(host=host, token=user_token)

    # Local dev: fall back to PAT token or environment-based service principal
    pat = os.environ.get("DATABRICKS_TOKEN", "").strip()
    if pat:
        return WorkspaceClient(host=host, token=pat)

    # Last resort: let the SDK discover credentials from the environment
    return WorkspaceClient()


# ---------------------------------------------------------------------------
# Warehouse ID
# ---------------------------------------------------------------------------

def _warehouse_id() -> str:
    path = os.environ.get("SQL_WAREHOUSE_HTTP_PATH", "").rstrip("/")
    wid  = path.split("/")[-1]

    if not wid or wid == "YOUR_WAREHOUSE_ID_HERE":
        raise RuntimeError(
            "SQL_WAREHOUSE_HTTP_PATH is not configured.\n\n"
            "Steps to fix:\n"
            "  1. Go to Databricks → SQL Warehouses\n"
            "  2. Click your warehouse → Connection Details tab\n"
            "  3. Copy the HTTP Path  (e.g. /sql/1.0/warehouses/abc1234def5678)\n"
            "  4. Update app.yaml → env → SQL_WAREHOUSE_HTTP_PATH\n"
            "  5. Redeploy the app"
        )
    return wid


# ---------------------------------------------------------------------------
# Query runner
# ---------------------------------------------------------------------------

def run_query(sql_text: str) -> pd.DataFrame:
    """
    Execute a SQL statement and return results as a DataFrame.
    Authenticates as the logged-in user when running inside Databricks Apps.
    """
    from databricks.sdk.service.sql import StatementState

    w   = _sdk_client()
    wid = _warehouse_id()

    resp = w.statement_execution.execute_statement(
        statement=sql_text,
        warehouse_id=wid,
        wait_timeout="50s",
    )

    state     = resp.status.state if resp.status else None
    state_str = state.value if hasattr(state, "value") else str(state)

    if state_str != "SUCCEEDED":
        error = getattr(resp.status, "error", None)
        msg   = error.message if error else state_str
        raise Exception(f"Query failed [{state_str}]: {msg}")

    if not resp.result or not resp.result.data_array:
        return pd.DataFrame()

    columns = [col.name for col in resp.manifest.schema.columns]
    rows    = [list(row) for row in resp.result.data_array]
    return pd.DataFrame(rows, columns=columns)
