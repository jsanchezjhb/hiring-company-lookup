"""
db_utils.py — Database connection matching the functional billing-disputes app.

Uses WorkspaceClient() with DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET.
User Authorization must be OFF — having it on injects DATABRICKS_TOKEN which
conflicts with the OAuth credentials and breaks the SDK.
"""

import os
import streamlit as st
import pandas as pd

_HOST      = os.environ.get("DATABRICKS_HOST",         "").strip()
_HTTP_PATH = os.environ.get("SQL_WAREHOUSE_HTTP_PATH", "").strip()


@st.cache_resource(show_spinner=False)
def _sdk_client():
    """Cached WorkspaceClient using service principal OAuth M2M."""
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient()


def _warehouse_id() -> str:
    wid = _HTTP_PATH.rstrip("/").split("/")[-1]
    if not wid or wid == "YOUR_WAREHOUSE_ID_HERE":
        raise RuntimeError(
            "SQL_WAREHOUSE_HTTP_PATH is not configured. "
            "Set it to /sql/1.0/warehouses/16984dfe9a2c3705 in your app environment."
        )
    return wid


def run_query(sql_text: str) -> pd.DataFrame:
    """Execute SQL via the Databricks SDK Statement Execution API."""
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
