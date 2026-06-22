"""
db_utils.py — Database connection utility for the Fraud Detection App.

Auth strategy (automatic, no code changes needed):
  Databricks App (production):
    Uses DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET injected by
    the platform. WorkspaceClient() discovers these automatically.

  Local dev:
    Set DATABRICKS_TOKEN to a Personal Access Token in your shell or .env.
    The SDK will prefer the PAT when it finds DATABRICKS_TOKEN.

Required env vars (only one you must set manually):
  SQL_WAREHOUSE_HTTP_PATH  e.g. /sql/1.0/warehouses/abc1234def5678
    → Databricks → SQL Warehouses → [warehouse] → Connection Details

Auto-injected by Databricks Apps (do not set manually in production):
  DATABRICKS_HOST, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET
"""

import os
import streamlit as st
import pandas as pd


# ---------------------------------------------------------------------------
# SDK client (cached for the app lifetime)
# ---------------------------------------------------------------------------

@st.cache_resource(show_spinner=False)
def _sdk_client():
    """
    Return a cached WorkspaceClient.
    Auto-discovers auth from environment variables — no configuration needed.
    """
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient()


def _warehouse_id() -> str:
    """
    Extract the warehouse ID from the SQL_WAREHOUSE_HTTP_PATH env var.
    /sql/1.0/warehouses/abc123  →  abc123
    """
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
    Execute a SQL statement via the Databricks Statement Execution API
    and return the results as a DataFrame.

    Uses the SDK WorkspaceClient, which handles OAuth M2M automatically
    when running as a Databricks App, and PAT auth for local dev.

    Args:
        sql_text: Databricks SQL string (company_id already substituted)

    Returns:
        pd.DataFrame — empty DataFrame if the query returns no rows

    Raises:
        RuntimeError  — if the warehouse path is not configured
        Exception     — if the query fails (message includes the SQL error)
    """
    from databricks.sdk.service.sql import StatementState

    w   = _sdk_client()
    wid = _warehouse_id()

    resp = w.statement_execution.execute_statement(
        statement=sql_text,
        warehouse_id=wid,
        wait_timeout="50s",
        on_wait_timeout="CANCEL",
    )

    if resp.status.state != StatementState.SUCCEEDED:
        error = getattr(resp.status, "error", None)
        msg   = error.message if error else str(resp.status.state)
        raise Exception(f"Query failed: {msg}")

    # No rows returned
    if not resp.result or not resp.result.data_array:
        return pd.DataFrame()

    columns = [col.name for col in resp.manifest.schema.columns]
    rows    = [list(row) for row in resp.result.data_array]
    return pd.DataFrame(rows, columns=columns)
