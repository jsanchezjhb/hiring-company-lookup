"""
db_utils.py — Database connection for the Fraud Detection App.

The Databricks Apps platform injects DATABRICKS_TOKEN (set to the user's
token when User Authorization is on) alongside DATABRICKS_CLIENT_ID and
DATABRICKS_CLIENT_SECRET. The SDK sees two auth methods and refuses to start.

Fix: save any PAT value before it's removed, always clear DATABRICKS_TOKEN
from the environment, then manage auth explicitly in _sdk_client().
"""

import os
import streamlit as st
import pandas as pd

# Save PAT value before cleanup — non-empty only in local dev
_saved_pat = os.environ.get("DATABRICKS_TOKEN", "").strip()

# Always clear DATABRICKS_TOKEN so the SDK never sees it alongside
# the OAuth credentials. Auth is handled explicitly below.
os.environ.pop("DATABRICKS_TOKEN", None)


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

def _sdk_client():
    """
    Build a WorkspaceClient using explicit credentials — no auto-discovery.

    Priority:
      1. X-Forwarded-Access-Token header  →  user's token (User Authorization)
      2. Saved PAT                        →  local dev
      3. No token                         →  OAuth M2M via env vars (production)
    """
    from databricks.sdk import WorkspaceClient

    host = os.environ.get("DATABRICKS_HOST", "").strip()

    # 1. User Authorization header (Databricks Apps)
    user_token = ""
    try:
        user_token = st.context.headers.get("X-Forwarded-Access-Token", "")
    except AttributeError:
        pass

    if user_token:
        return WorkspaceClient(host=host, token=user_token)

    # 2. PAT for local dev
    if _saved_pat:
        return WorkspaceClient(host=host, token=_saved_pat)

    # 3. OAuth M2M — DATABRICKS_CLIENT_ID + SECRET are still in env,
    #    DATABRICKS_TOKEN is gone, so no conflict
    return WorkspaceClient()


# ---------------------------------------------------------------------------
# Warehouse ID
# ---------------------------------------------------------------------------

def _warehouse_id() -> str:
    path = os.environ.get("SQL_WAREHOUSE_HTTP_PATH", "").rstrip("/")
    wid  = path.split("/")[-1]
    if not wid or wid == "YOUR_WAREHOUSE_ID_HERE":
        raise RuntimeError(
            "SQL_WAREHOUSE_HTTP_PATH is not configured.\n"
            "Set it to the HTTP Path from your SQL Warehouse Connection Details."
        )
    return wid


# ---------------------------------------------------------------------------
# Query runner
# ---------------------------------------------------------------------------

def run_query(sql_text: str) -> pd.DataFrame:
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
