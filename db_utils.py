"""
db_utils.py — Database connection for the Fraud Detection App.

Auth note: Databricks Apps injects DATABRICKS_TOKEN="" (empty string) into
the environment even when OAuth M2M credentials are present. The SDK treats
any DATABRICKS_TOKEN entry — even empty — as a PAT auth method, which
conflicts with DATABRICKS_CLIENT_ID/SECRET (OAuth). We remove it at import
time if empty so the SDK only sees one auth method.
"""

import os
import streamlit as st
import pandas as pd

# Remove empty DATABRICKS_TOKEN before the SDK initialises.
# Databricks Apps sets this to "" by default; the SDK then sees both PAT
# (empty token) and OAuth (client_id + client_secret) and refuses to start.
# Only remove it when empty — a real PAT for local dev is left untouched.
if not os.environ.get("DATABRICKS_TOKEN", ""):
    os.environ.pop("DATABRICKS_TOKEN", None)


# ---------------------------------------------------------------------------
# Client — per user, not cached globally
# ---------------------------------------------------------------------------

def _sdk_client():
    """
    Return a WorkspaceClient authenticated as the current user.

    When Databricks Apps User Authorization is enabled, the logged-in user's
    token arrives in the X-Forwarded-Access-Token header. We use that token
    so Unity Catalog queries run as the user, not the service principal.

    If no user token is found (local dev, or User Authorization not enabled),
    WorkspaceClient() is called with NO arguments so the SDK auto-discovers
    auth from DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET env vars.
    Passing an empty token= alongside OAuth env vars causes a conflict, so
    we only pass token when it is genuinely non-empty.
    """
    from databricks.sdk import WorkspaceClient

    user_token = ""
    try:
        user_token = st.context.headers.get("X-Forwarded-Access-Token", "")
    except AttributeError:
        # st.context.headers requires Streamlit >= 1.37
        pass

    if user_token:
        host = os.environ.get("DATABRICKS_HOST", "").strip()
        return WorkspaceClient(host=host, token=user_token)

    # No user token — let SDK pick up OAuth or PAT from env vars automatically
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
