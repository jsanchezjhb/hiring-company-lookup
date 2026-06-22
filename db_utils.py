"""
db_utils.py — Database connection utilities for the Hiring Fraud Detection App.

Connects to a Databricks SQL Warehouse using:
  - Local dev:  PAT token via DATABRICKS_TOKEN env var
  - Production: OAuth auto-auth when running as a Databricks App

Required environment variables:
  DATABRICKS_HOST          e.g.  homebase-staging.cloud.databricks.com
  SQL_WAREHOUSE_HTTP_PATH  e.g.  /sql/1.0/warehouses/abc123
  DATABRICKS_TOKEN         (local dev only — leave unset in Databricks Apps)
"""

import os
import streamlit as st
import pandas as pd


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

@st.cache_resource(show_spinner=False)
def get_db_connection():
    """
    Create and cache a single Databricks SQL connection for the app lifetime.
    Uses PAT token locally; falls back to OAuth inside a Databricks App.
    """
    try:
        from databricks import sql as dbsql
    except ImportError:
        raise RuntimeError(
            "databricks-sql-connector is not installed.\n"
            "Run:  pip install databricks-sql-connector"
        )

    host = os.environ.get("DATABRICKS_HOST", "").strip()
    http_path = os.environ.get("SQL_WAREHOUSE_HTTP_PATH", "").strip()
    token = os.environ.get("DATABRICKS_TOKEN", "").strip()

    if not host or not http_path:
        raise RuntimeError(
            "Missing required environment variables.\n"
            "Set DATABRICKS_HOST and SQL_WAREHOUSE_HTTP_PATH."
        )

    conn_kwargs = dict(
        server_hostname=host,
        http_path=http_path,
    )
    if token:
        conn_kwargs["access_token"] = token
    # If no token is provided, the connector will use Databricks OAuth
    # automatically when the app is running inside Databricks Apps.

    return dbsql.connect(**conn_kwargs)


# ---------------------------------------------------------------------------
# Query runner
# ---------------------------------------------------------------------------

def run_query(sql: str) -> pd.DataFrame:
    """
    Execute a SQL statement against the warehouse and return a DataFrame.

    Args:
        sql: Databricks SQL string (company_id already substituted by caller)

    Returns:
        pd.DataFrame — empty if the query returns no rows

    Raises:
        Exception — propagated to the caller so the UI can show an error card
    """
    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()
        cols = [desc[0] for desc in cursor.description] if cursor.description else []
        return pd.DataFrame(rows, columns=cols)
