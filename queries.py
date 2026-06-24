"""
queries.py — Fraud detection signal queries for the Hiring product.

Each signal function:
  - Takes a company_id (int)
  - Returns a result dict: { status, message, detail_df, alert_count }

Status values:
  "ALERT"   — suspicious behaviour detected
  "CLEAR"   — no issues found
  "PENDING" — not yet implemented (needs additional info)
  "ERROR"   — query failed (detail in message)
"""

from __future__ import annotations

import os
import streamlit as st
from typing import Any, Dict
import pandas as pd

# Hardcoded exactly like the billing-disputes app — avoids any timing issue
# with module-level env var reads before Databricks sets them.
DATABRICKS_HOST      = "homebase-staging.cloud.databricks.com"
DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/16984dfe9a2c3705"


def _user_token() -> str:
    try:
        t = st.context.headers.get("X-Forwarded-Access-Token", "").strip()
        if t:
            return t
    except AttributeError:
        pass
    return os.environ.get("DATABRICKS_TOKEN", "").strip()


def run_query(sql_text: str) -> pd.DataFrame:
    from databricks import sql as dbsql

    token = _user_token()
    if not token:
        raise RuntimeError(
            "No user token found. "
            "Enable User Authorization in Apps → Edit → User Authorization."
        )

def run_query(sql_text: str) -> pd.DataFrame:
    from databricks.sdk.core import Config
    from databricks import sql as dbsql

    # Exact pattern from the working billing-disputes app:
    #   cfg = Config()  →  discovers DATABRICKS_CLIENT_ID + SECRET (OAuth M2M)
    #   credentials_provider=lambda: cfg.authenticate  →  two-level Thrift auth
    # User Authorization must be OFF so DATABRICKS_TOKEN is not injected
    # alongside the OAuth credentials (that causes the "two auth methods" conflict).
    cfg  = Config()
    conn = dbsql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        credentials_provider=lambda: cfg.authenticate,
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_text)
            rows = cursor.fetchall()
            cols = [d[0] for d in cursor.description] if cursor.description else []
            df = pd.DataFrame(rows, columns=cols)
            # Deduplicate at the source so every signal counts and displays clean data.
            df = df.drop_duplicates().reset_index(drop=True)
            # Convert ID columns to plain strings so Streamlit doesn't add comma separators.
            # Handles integer, float (.0), and already-string IDs (e.g. Stripe cus_ IDs).
            id_cols = [c for c in df.columns if c == "id" or c.endswith("_id")]
            for col in id_cols:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace(r"\.0$", "", regex=True)
                    .replace("nan", "")
                )
            return df
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

EXCLUDE_TEST_COMPANY = "AND c.company_id != 1987234"


def _result(status: str, message: str, df: pd.DataFrame, count: int | None = None) -> Dict[str, Any]:
    return {
        "status": status,
        "message": message,
        "detail_df": df,
        "alert_count": count if count is not None else len(df),
    }


def _pending(reason: str) -> Dict[str, Any]:
    return _result("PENDING", reason, pd.DataFrame(), 0)


def _error(exc: Exception) -> Dict[str, Any]:
    return _result("ERROR", f"Query error: {exc}", pd.DataFrame(), 0)


# ---------------------------------------------------------------------------
# Company lookup
# ---------------------------------------------------------------------------

def get_company_info(company_id: int) -> pd.DataFrame:
    """Fetch basic company metadata for the header card."""
    sql = f"""
    SELECT
        c.company_id,
        c.name                           AS company_name,
        c.uuid                           AS company_uuid,
        CAST(c.created_at AS DATE)       AS member_since,
        COALESCE(c.employee_count, 0)    AS employee_count,
        COALESCE(c.location_count, 0)    AS location_count
    FROM public.companies c
    WHERE c.company_id = {company_id}
    """
    return run_query(sql)


# ---------------------------------------------------------------------------
# Signal 1 — 10+ Active Job Posts
# ---------------------------------------------------------------------------

def check_active_job_posts(company_id: int) -> Dict[str, Any]:
    """
    ALERT if the company currently has 10 or more active job posts
    across all of its locations.
    """
    THRESHOLD = 10
    sql = f"""
    SELECT
        hjr.id                                          AS job_post_id,
        hjr.title                                       AS job_title,
        hjr.status,
        CAST(hjr.created_at AS DATE)                    AS created_date,
        CAST(hjr.activated_at AS TIMESTAMP)             AS activated_at,
        l.location_id,
        l.name                                          AS location_name,
        c.company_id
    FROM postgres.hiring_job_requests hjr
    INNER JOIN public.locations  l ON l.location_id = hjr.location_id
    INNER JOIN public.companies  c ON c.company_id  = l.company_id
    WHERE c.company_id     = {company_id}
        AND hjr.hiring_version = 2
        AND hjr.status         = 'active'
        AND hjr.activated_at   IS NOT NULL
        {EXCLUDE_TEST_COMPANY}
    ORDER BY hjr.activated_at DESC
    """
    try:
        df    = run_query(sql)
        count = len(df)
        flagged = count >= THRESHOLD
        msg = (
            f"{count} active job post{'s' if count != 1 else ''} — exceeds threshold of {THRESHOLD}"
            if flagged else
            f"{count} active job post{'s' if count != 1 else ''} — within normal range"
        )
        return _result("ALERT" if flagged else "CLEAR", msg, df, count)
    except Exception as exc:
        return _error(exc)


# ---------------------------------------------------------------------------
# Signal 2 — Jobs Posted Less Than 1 Minute Apart
# ---------------------------------------------------------------------------

def check_rapid_postings(company_id: int) -> Dict[str, Any]:
    """
    ALERT if any two consecutive job posts from this company were created
    less than 60 seconds apart — consistent with automation or bulk fraud.
    created_at is a TIMESTAMP; gap is computed with UNIX_TIMESTAMP().
    """
    sql = f"""
    WITH job_sequence AS (
        SELECT
            hjr.id          AS job_post_id,
            hjr.title       AS job_title,
            hjr.status,
            hjr.created_at,
            LAG(hjr.created_at) OVER (
                PARTITION BY c.company_id
                ORDER BY hjr.created_at ASC
            )               AS prev_created_at,
            l.location_id,
            l.name          AS location_name
        FROM postgres.hiring_job_requests hjr
        INNER JOIN public.locations  l ON l.location_id = hjr.location_id
        INNER JOIN public.companies  c ON c.company_id  = l.company_id
        WHERE c.company_id     = {company_id}
            AND hjr.hiring_version = 2
            AND hjr.status        != 'draft'
            AND (hjr.activated_at IS NOT NULL OR hjr.flagged_at IS NOT NULL)
            {EXCLUDE_TEST_COMPANY}
    )
    SELECT
        job_post_id,
        job_title,
        status,
        created_at,
        prev_created_at,
        CAST(UNIX_TIMESTAMP(created_at) - UNIX_TIMESTAMP(prev_created_at) AS INT)
                        AS seconds_between_posts,
        ROUND((UNIX_TIMESTAMP(created_at) - UNIX_TIMESTAMP(prev_created_at)) / 60.0, 2)
                        AS minutes_between_posts,
        location_id,
        location_name
    FROM job_sequence
    WHERE prev_created_at IS NOT NULL
        AND (UNIX_TIMESTAMP(created_at) - UNIX_TIMESTAMP(prev_created_at)) < 60
    ORDER BY created_at DESC
    """
    try:
        df      = run_query(sql)
        count   = len(df)
        flagged = count > 0
        msg = (
            f"{count} instance{'s' if count != 1 else ''} of jobs posted less than 1 minute apart"
            if flagged else
            "No sub-minute posting bursts detected"
        )
        return _result("ALERT" if flagged else "CLEAR", msg, df, count)
    except Exception as exc:
        return _error(exc)


# ---------------------------------------------------------------------------
# Signal 3 — 4+ Jobs Posted Within a Single Hour
# ---------------------------------------------------------------------------

def check_hourly_burst(company_id: int) -> Dict[str, Any]:
    """
    ALERT if the company posted 4 or more jobs within any single clock-hour.

    Returns one row per job that falls inside a flagged hour window,
    so the rep can see exactly which jobs were posted together.
    """
    THRESHOLD = 4

    sql = f"""
    WITH job_hours AS (
        SELECT
            hjr.id                                             AS job_post_id,
            hjr.title                                          AS job_title,
            hjr.status,
            hjr.created_at                                   AS created_at,
            DATE_TRUNC('HOUR', hjr.created_at)               AS hour_bucket,
            l.location_id,
            l.name                                             AS location_name
        FROM postgres.hiring_job_requests hjr
        INNER JOIN public.locations  l ON l.location_id = hjr.location_id
        INNER JOIN public.companies  c ON c.company_id  = l.company_id
        WHERE c.company_id     = {company_id}
            AND hjr.hiring_version = 2
            AND hjr.status        != 'draft'
            AND (hjr.activated_at IS NOT NULL OR hjr.flagged_at IS NOT NULL)
            {EXCLUDE_TEST_COMPANY}
    ),
    flagged_hours AS (
        SELECT
            hour_bucket,
            COUNT(*) AS jobs_in_hour
        FROM job_hours
        GROUP BY hour_bucket
        HAVING COUNT(*) >= {THRESHOLD}
    )
    SELECT
        jh.hour_bucket,
        fh.jobs_in_hour                   AS total_jobs_in_hour,
        jh.job_post_id,
        jh.job_title,
        jh.status,
        jh.created_at,
        jh.location_id,
        jh.location_name
    FROM job_hours jh
    INNER JOIN flagged_hours fh ON fh.hour_bucket = jh.hour_bucket
    ORDER BY jh.hour_bucket DESC, jh.created_at ASC
    """
    try:
        df           = run_query(sql)
        hours_flagged = df["hour_bucket"].nunique() if not df.empty else 0
        total_jobs    = len(df)
        flagged       = hours_flagged > 0
        msg = (
            f"{hours_flagged} hour window{'s' if hours_flagged != 1 else ''} with 4+ posts "
            f"({total_jobs} total jobs flagged)"
            if flagged else
            "No hourly burst activity detected"
        )
        return _result("ALERT" if flagged else "CLEAR", msg, df, hours_flagged)
    except Exception as exc:
        return _error(exc)


# ---------------------------------------------------------------------------
# Signal 4 — IP / Location Mismatch
# ---------------------------------------------------------------------------
#
# SIGNUP DATA SOURCES
# ────────────────────
#   _IP_SIGNUP_TABLE : Heap events (pre-2023)
#                      prod_redshift_replica.heap.sign_up_owner_signed_up
#                      columns: ip, city, region, country, time
#
#   _AMPLITUDE_TABLE : Amplitude events (post-2023)
#                      prod_redshift_replica.dbt_staging.s_amp_owner_signups_raw
#                      (workaround — prod_enriched.amplitude table is restricted)
#                      columns: ip_address, city, region, country, event_time
#
#   Both sources are combined via UNION ALL in the signup_geo CTE so every
#   signup — regardless of when it happened — is evaluated.
#
#   _LOCATION_TABLE  : prod_redshift_replica.postgres.locations
#                      columns: id, company_id, city, state, zip, address_1
#
# HOW location_match_status IS SCORED
# ─────────────────────────────────────
#   'City Match'       →  ip_city == provided_city          (CLEAR)
#   'State Match Only' →  ip_state == provided_state only   (surfaced, not ALERTed)
#   'No Match'         →  neither city nor state matches    (ALERT)
#
# mismatch_pct heuristic:
#   0%   exact city match
#   10%  partial city name overlap
#   25%  known CDN/cloud hub city with state mismatch (Ashburn, Atlanta, etc.)
#   30%  state match only
#   60%  US country match, city/state mismatch
#   65%  Canada, city/state mismatch
#   90%  foreign country
# ---------------------------------------------------------------------------

_IP_SIGNUP_TABLE = "prod_redshift_replica.heap.sign_up_owner_signed_up"          # pre-2023 signups
_AMPLITUDE_TABLE = "prod_redshift_replica.dbt_staging.s_amp_owner_signups_raw"   # post-2023 signups (workaround)
_LOCATION_TABLE  = "prod_redshift_replica.postgres.locations"


def check_ip_location_mismatch(company_id: int) -> Dict[str, Any]:
    """
    ALERT if the IP address at account creation does not align with the
    company's registered city or state.

    Uses prod_redshift_replica.postgres.locations for the registered address.
    Returns ALL rows so the rep sees City Match, State Match Only, and No Match.
    ALERTs when at least one row is 'No Match'.
    """
    sql = f"""
    WITH state_abbrev AS (
        SELECT abbr, full_name FROM (VALUES
            ('AL','Alabama'),('AK','Alaska'),('AZ','Arizona'),('AR','Arkansas'),
            ('CA','California'),('CO','Colorado'),('CT','Connecticut'),('DE','Delaware'),
            ('FL','Florida'),('GA','Georgia'),('HI','Hawaii'),('ID','Idaho'),
            ('IL','Illinois'),('IN','Indiana'),('IA','Iowa'),('KS','Kansas'),
            ('KY','Kentucky'),('LA','Louisiana'),('ME','Maine'),('MD','Maryland'),
            ('MA','Massachusetts'),('MI','Michigan'),('MN','Minnesota'),('MS','Mississippi'),
            ('MO','Missouri'),('MT','Montana'),('NE','Nebraska'),('NV','Nevada'),
            ('NH','New Hampshire'),('NJ','New Jersey'),('NM','New Mexico'),('NY','New York'),
            ('NC','North Carolina'),('ND','North Dakota'),('OH','Ohio'),('OK','Oklahoma'),
            ('OR','Oregon'),('PA','Pennsylvania'),('RI','Rhode Island'),('SC','South Carolina'),
            ('SD','South Dakota'),('TN','Tennessee'),('TX','Texas'),('UT','Utah'),
            ('VT','Vermont'),('VA','Virginia'),('WA','Washington'),('WV','West Virginia'),
            ('WI','Wisconsin'),('WY','Wyoming'),('DC','District of Columbia'),
            ('AB','Alberta'),('BC','British Columbia'),('MB','Manitoba'),
            ('NB','New Brunswick'),('NL','Newfoundland and Labrador'),('NS','Nova Scotia'),
            ('NT','Northwest Territories'),('NU','Nunavut'),('ON','Ontario'),
            ('PE','Prince Edward Island'),('QC','Quebec'),('SK','Saskatchewan'),
            ('YT','Yukon')
        ) AS t(abbr, full_name)
    ),
    signup_geo AS (
        -- Heap (pre-2023)
        SELECT
            company_id,
            location_id,
            ip,
            city    AS ip_city,
            region  AS ip_region,
            country AS ip_country,
            time    AS signup_time
        FROM {_IP_SIGNUP_TABLE}
        WHERE ip IS NOT NULL
          AND company_id = {company_id}

        UNION ALL

        -- Amplitude (post-2023)
        SELECT
            company_id,
            location_id,
            ip_address  AS ip,
            city        AS ip_city,
            region      AS ip_region,
            country     AS ip_country,
            event_time  AS signup_time
        FROM {_AMPLITUDE_TABLE}
        WHERE ip_address IS NOT NULL
          AND company_id = {company_id}
    ),
    location_address AS (
        SELECT
            id          AS location_id,
            company_id,
            city        AS provided_city,
            state       AS provided_state,
            zip,
            address_1
        FROM {_LOCATION_TABLE}
        WHERE city IS NOT NULL
          AND company_id = {company_id}
    )
    SELECT
        sg.company_id,
        sg.ip,
        sg.ip_city,
        sg.ip_region,
        sg.ip_country,
        la.provided_city,
        la.provided_state,
        la.zip,
        COALESCE(sa_ip.abbr, LOWER(TRIM(sg.ip_region))) AS ip_state_normalized,
        COALESCE(LOWER(sa_loc.full_name), LOWER(TRIM(la.provided_state))) AS provided_state_normalized,
        CASE
            WHEN LOWER(TRIM(sg.ip_city)) = LOWER(TRIM(la.provided_city))
                THEN 'City Match'
            WHEN COALESCE(LOWER(sa_ip.abbr), LOWER(TRIM(sg.ip_region)))
               = COALESCE(LOWER(la.provided_state), '')
              OR LOWER(TRIM(sg.ip_region)) = COALESCE(LOWER(sa_loc.full_name), '')
                THEN 'State Match Only'
            ELSE 'No Match'
        END AS location_match_status,
        CONCAT(
            CASE
                WHEN LOWER(TRIM(sg.ip_city)) = LOWER(TRIM(la.provided_city)) THEN '0'
                WHEN LOWER(la.provided_city) LIKE '%' || LOWER(TRIM(sg.ip_city)) || '%'
                  OR LOWER(TRIM(sg.ip_city)) LIKE '%' || LOWER(TRIM(la.provided_city)) || '%'
                    THEN '10'
                WHEN LOWER(TRIM(sg.ip_city)) IN ('ashburn','atlanta','chicago','dallas','seattle','los angeles','san jose')
                  AND COALESCE(LOWER(sa_ip.abbr), LOWER(TRIM(sg.ip_region))) != LOWER(TRIM(la.provided_state))
                  AND LOWER(TRIM(sg.ip_region)) != COALESCE(LOWER(sa_loc.full_name), '')
                    THEN '25'
                WHEN COALESCE(LOWER(sa_ip.abbr), LOWER(TRIM(sg.ip_region))) = LOWER(TRIM(la.provided_state))
                  OR LOWER(TRIM(sg.ip_region)) = COALESCE(LOWER(sa_loc.full_name), '')
                    THEN '30'
                WHEN LOWER(TRIM(sg.ip_country)) = 'united states' THEN '60'
                WHEN LOWER(TRIM(sg.ip_country)) IN ('canada')      THEN '65'
                WHEN LOWER(TRIM(sg.ip_country)) NOT IN ('united states','canada') THEN '90'
                ELSE '50'
            END,
            '%'
        ) AS mismatch_pct,
        sg.signup_time
    FROM signup_geo sg
    JOIN location_address la ON sg.company_id = la.company_id
    LEFT JOIN state_abbrev sa_ip  ON LOWER(TRIM(sg.ip_region))      = LOWER(sa_ip.full_name)
    LEFT JOIN state_abbrev sa_loc ON LOWER(TRIM(la.provided_state)) = LOWER(sa_loc.abbr)
    ORDER BY sg.signup_time DESC
    """
    try:
        df = run_query(sql)

        if df.empty:
            # No geo data — look up how the company was created as context
            try:
                co_df = run_query(f"""
                    SELECT onboard_source
                    FROM {_CO_TABLE}
                    WHERE company_id = {company_id}
                    LIMIT 1
                """)
                onboard = (
                    co_df["onboard_source"].iloc[0]
                    if not co_df.empty and co_df["onboard_source"].iloc[0] not in (None, "", "None")
                    else "unknown"
                )
            except Exception:
                onboard = "unknown"
            return _result(
                "CLEAR",
                f"No signup geo data found — company onboard source: {onboard}",
                pd.DataFrame(),
                0,
            )

        # Count by status
        status_counts = (
            df["location_match_status"].value_counts().to_dict()
            if "location_match_status" in df.columns
            else {}
        )
        no_match_count    = status_counts.get("No Match",       0)
        state_only_count  = status_counts.get("State Match Only", 0)
        city_match_count  = status_counts.get("City Match",     0)

        flagged = no_match_count > 0

        parts = []
        if no_match_count:
            parts.append(f"{no_match_count} No Match")
        if state_only_count:
            parts.append(f"{state_only_count} State Match Only")
        if city_match_count:
            parts.append(f"{city_match_count} City Match")

        msg = "  |  ".join(parts) if parts else "No rows returned"

        return _result("ALERT" if flagged else "CLEAR", msg, df, no_match_count)

    except Exception as exc:
        return _error(exc)


# ---------------------------------------------------------------------------
# Signal 5 — Dormancy Reactivation (30+ Day Gap Before First Hiring Post)
# ---------------------------------------------------------------------------

_SIGNIN_TABLE = "prod_redshift_replica.postgres.account_sign_in_details"

def check_dormancy_reactivation(company_id: int) -> Dict[str, Any]:
    """
    ALERT if the company had zero sign-in activity for 30+ days before their
    first Hiring job post — indicating the account was dormant then suddenly
    activated for Hiring.

    last_signin_at  = most recent sign-in across all company accounts,
                      using GREATEST(current_sign_in_at, last_sign_in_at)
    first_post_at   = earliest activated hiring_job_request for this company
    Gap             = DATEDIFF(first_post_at, last_signin_at)
    """
    sql = f"""
    WITH company_account_ids AS (
        SELECT DISTINCT j.user_id AS account_id
        FROM {_LOCS_TABLE} l
        JOIN {_JOBS_TABLE} j ON j.location_id = l.id
        WHERE l.company_id = {company_id}
    ),
    last_signin AS (
        SELECT GREATEST(
            MAX(sd.current_sign_in_at),
            MAX(sd.last_sign_in_at)
        ) AS last_signin_at
        FROM {_SIGNIN_TABLE} sd
        INNER JOIN company_account_ids ca ON ca.account_id = sd.account_id
    ),
    first_job AS (
        SELECT MIN(hjr.created_at) AS first_post_at
        FROM postgres.hiring_job_requests hjr
        INNER JOIN public.locations l ON l.location_id = hjr.location_id
        INNER JOIN public.companies c ON c.company_id  = l.company_id
        WHERE c.company_id        = {company_id}
          AND hjr.hiring_version  = 2
          AND hjr.status         != 'draft'
          AND (hjr.activated_at IS NOT NULL OR hjr.flagged_at IS NOT NULL)
          {EXCLUDE_TEST_COMPANY}
    )
    SELECT
        ls.last_signin_at,
        fj.first_post_at,
        DATEDIFF(fj.first_post_at, ls.last_signin_at) AS gap_days
    FROM last_signin ls
    CROSS JOIN first_job fj
    WHERE fj.first_post_at IS NOT NULL
    """
    try:
        df = run_query(sql)

        if df.empty or df["first_post_at"].isna().all():
            return _result("PENDING", "No hiring job posts found for this company", df, 0)

        last_signin = df["last_signin_at"].iloc[0]
        first_post  = df["first_post_at"].iloc[0]
        gap         = df["gap_days"].iloc[0]

        if last_signin is None or str(last_signin) in ("", "NaT", "None"):
            return _result(
                "ALERT",
                f"No account sign-ins found before first Hiring post ({first_post}) — zero prior activity",
                df, 0,
            )

        gap_int = int(gap) if gap is not None else 0

        if gap_int >= 30:
            return _result(
                "ALERT",
                f"{gap_int}-day gap between last sign-in ({last_signin}) and first Hiring post ({first_post})",
                df, gap_int,
            )

        return _result(
            "CLEAR",
            f"Account was active before Hiring — {gap_int} day gap before first job post",
            df, gap_int,
        )
    except Exception as exc:
        return _error(exc)


# ===========================================================================
# Stripe constants — prod_redshift_replica.stripe.*
# ===========================================================================
# Tables confirmed from SHOW TABLES IN prod_redshift_replica.stripe
# Company linkage: GET_JSON_OBJECT(metadata, '$.company_id') on customer_subscription

_SUB_TABLE     = "prod_redshift_replica.stripe.customer_subscription"  # company → customer
_CHG_TABLE     = "prod_redshift_replica.stripe.charge"                 # Signals 6, 7, 8, 9
_DISPUTE_TABLE = "prod_redshift_replica.stripe.i_charge_dispute"       # Signal 7 — direct dispute lookup
_CUST_TABLE    = "prod_redshift_replica.stripe.customer"               # Signal 15 customer lookup
_CUST_SRC_TABLE= "prod_redshift_replica.stripe.customer_source"        # Signals 9, 15 — stored cards (fingerprint col)
_PM_TABLE      = "prod_redshift_replica.stripe.payment_method"         # Signals 9, 15 — PaymentMethod API cards
_CO_TABLE      = "prod_redshift_replica.public.companies"              # Signal 9 name lookup

# customer_subscription.metadata contains company_id as a JSON string:
#   {"company_id":"1311802","location_id":"1415801", ...}
_META_CO_ID = "GET_JSON_OBJECT(metadata, '$.company_id')"

# charge.customer = Stripe cus_* ID
# charge.created  = Unix epoch seconds → FROM_UNIXTIME()
# Fingerprint     = GET_JSON_OBJECT(payment_method_details, '$.card.fingerprint')
_FINGERPRINT_PATH = "'$.card.fingerprint'"
_PM_THRESHOLD     = 2


# Company customers CTE — shared by Signals 6, 7, 8, 9
def _company_customers_cte(company_id: int) -> str:
    return f"""
    company_customers AS (
        SELECT DISTINCT customer AS stripe_customer
        FROM {_SUB_TABLE}
        WHERE {_META_CO_ID} = '{company_id}'
          AND customer IS NOT NULL
    )"""


# ---------------------------------------------------------------------------
# Signal 6 — Failed / Unsuccessful Billing
# ---------------------------------------------------------------------------

def check_failed_billing(company_id: int) -> Dict[str, Any]:
    """ALERT if the company has any failed Stripe charges."""
    sql = f"""
    WITH {_company_customers_cte(company_id)}
    SELECT
        c.id                            AS charge_id,
        ROUND(c.amount / 100.0, 2)      AS amount_usd,
        UPPER(c.currency)               AS currency,
        c.status,
        COALESCE(c.failure_code,    'n/a') AS failure_code,
        COALESCE(c.failure_message, 'n/a') AS failure_message,
        FROM_UNIXTIME(c.created)        AS charged_at
    FROM {_CHG_TABLE} c
    INNER JOIN company_customers cc ON cc.stripe_customer = c.customer
    WHERE c.status = 'failed'
    ORDER BY c.created DESC
    """
    try:
        df      = run_query(sql)
        count   = len(df)
        flagged = count > 0
        msg = (
            f"{count} failed charge{'s' if count != 1 else ''}"
            if flagged else
            "No failed charges found"
        )
        return _result("ALERT" if flagged else "CLEAR", msg, df, count)
    except Exception as exc:
        return _error(exc)


# ---------------------------------------------------------------------------
# Signal 7 — Billing Disputes
# ---------------------------------------------------------------------------

def check_billing_disputes(company_id: int) -> Dict[str, Any]:
    """
    ALERT if any Stripe dispute exists for this company.
    Joins directly on customer_id — no charge table join needed.
    Column reference confirmed via DESCRIBE i_charge_dispute.
    """
    sql = f"""
    WITH {_company_customers_cte(company_id)}
    SELECT
        d.dispute_id,
        d.charge_id,
        d.amount,
        d.status               AS dispute_status,
        d.reason,
        d.customer_name,
        d.customer_email,
        d.created_at           AS disputed_at,
        d.evidence_due_date
    FROM {_DISPUTE_TABLE} d
    INNER JOIN company_customers cc ON cc.stripe_customer = d.customer_id
    ORDER BY d.created_at DESC
    """
    try:
        df      = run_query(sql)
        count   = len(df)
        flagged = count > 0
        msg = (
            f"{count} dispute{'s' if count != 1 else ''} found"
            if flagged else
            "No billing disputes found"
        )
        return _result("ALERT" if flagged else "CLEAR", msg, df, count)
    except Exception as exc:
        return _error(exc)


# ---------------------------------------------------------------------------
# Signal 8 — Excessive Payment Method Changes
# ---------------------------------------------------------------------------

def check_payment_method_changes(company_id: int) -> Dict[str, Any]:
    """ALERT if the company has used more than 2 distinct payment methods."""
    sql = f"""
    WITH {_company_customers_cte(company_id)}
    SELECT
        c.payment_method                AS payment_method_id,
        COUNT(*)                        AS times_charged,
        ROUND(SUM(c.amount) / 100.0, 2) AS total_charged_usd,
        MIN(FROM_UNIXTIME(c.created))   AS first_used_at,
        MAX(FROM_UNIXTIME(c.created))   AS last_used_at
    FROM {_CHG_TABLE} c
    INNER JOIN company_customers cc ON cc.stripe_customer = c.customer
    WHERE c.payment_method IS NOT NULL
    GROUP BY c.payment_method
    ORDER BY first_used_at DESC
    """
    try:
        df           = run_query(sql)
        distinct_pms = len(df)
        flagged      = distinct_pms > _PM_THRESHOLD
        msg = (
            f"{distinct_pms} distinct payment methods — exceeds threshold of {_PM_THRESHOLD}"
            if flagged else
            f"{distinct_pms} distinct payment method{'s' if distinct_pms != 1 else ''} — within normal range"
        )
        return _result("ALERT" if flagged else "CLEAR", msg, df, distinct_pms)
    except Exception as exc:
        return _error(exc)


# ---------------------------------------------------------------------------
# Signal 9 — Stripe Card Fingerprint Reuse Across Companies
# ---------------------------------------------------------------------------

def check_fingerprint_reuse(company_id: int) -> Dict[str, Any]:
    """
    ALERT if any card fingerprint associated with this company's Stripe customer
    also appears on another company's customer.

    Checks three sources so saved-but-uncharged cards are also caught:
      1. customer_source.fingerprint     — legacy stored cards (direct column)
      2. payment_method.card fingerprint — PaymentMethod API cards (JSON)
      3. charge payment_method_details   — historical charge fingerprints

    row_deleted_at IS NULL filters active records in customer_source / payment_method.
    Charge scan is limited to 6 months to avoid a full table scan.
    """
    sql = f"""
    WITH {_company_customers_cte(company_id)},

    target_fps AS (
        SELECT DISTINCT fingerprint
        FROM {_CUST_SRC_TABLE}
        WHERE customer IN (SELECT stripe_customer FROM company_customers)
          AND fingerprint IS NOT NULL
          AND row_deleted_at IS NULL

        UNION

        SELECT DISTINCT GET_JSON_OBJECT(card, '$.fingerprint') AS fingerprint
        FROM {_PM_TABLE}
        WHERE customer IN (SELECT stripe_customer FROM company_customers)
          AND GET_JSON_OBJECT(card, '$.fingerprint') IS NOT NULL
          AND row_deleted_at IS NULL

        UNION

        SELECT DISTINCT GET_JSON_OBJECT(payment_method_details, {_FINGERPRINT_PATH}) AS fingerprint
        FROM {_CHG_TABLE}
        WHERE customer IN (SELECT stripe_customer FROM company_customers)
          AND GET_JSON_OBJECT(payment_method_details, {_FINGERPRINT_PATH}) IS NOT NULL
    ),

    other_customers AS (
        SELECT DISTINCT cs.customer, tf.fingerprint, 'stored_card' AS match_source
        FROM {_CUST_SRC_TABLE} cs
        INNER JOIN target_fps tf ON tf.fingerprint = cs.fingerprint
        WHERE cs.customer NOT IN (SELECT stripe_customer FROM company_customers)
          AND cs.row_deleted_at IS NULL

        UNION

        SELECT DISTINCT pm.customer, tf.fingerprint, 'payment_method' AS match_source
        FROM {_PM_TABLE} pm
        INNER JOIN target_fps tf ON tf.fingerprint = GET_JSON_OBJECT(pm.card, '$.fingerprint')
        WHERE pm.customer NOT IN (SELECT stripe_customer FROM company_customers)
          AND pm.row_deleted_at IS NULL

        UNION

        SELECT DISTINCT c.customer, tf.fingerprint, 'charge' AS match_source
        FROM {_CHG_TABLE} c
        INNER JOIN target_fps tf ON tf.fingerprint
                                  = GET_JSON_OBJECT(c.payment_method_details, {_FINGERPRINT_PATH})
        WHERE c.customer NOT IN (SELECT stripe_customer FROM company_customers)
          AND c.created >= UNIX_TIMESTAMP() - 15768000
    )

    SELECT
        oc.fingerprint,
        oc.customer                                              AS other_stripe_customer,
        oc.match_source,
        GET_JSON_OBJECT(s.metadata, '$.company_id')             AS other_company_id,
        co.name                                                  AS other_company_name
    FROM other_customers oc
    INNER JOIN {_SUB_TABLE} s ON s.customer = oc.customer
    LEFT JOIN {_CO_TABLE} co
           ON CAST(co.company_id AS STRING) = GET_JSON_OBJECT(s.metadata, '$.company_id')
    WHERE GET_JSON_OBJECT(s.metadata, '$.company_id') IS NOT NULL
      AND GET_JSON_OBJECT(s.metadata, '$.company_id') != '{company_id}'
    ORDER BY oc.fingerprint, oc.match_source
    LIMIT 100
    """
    try:
        df         = run_query(sql)

        # Deduplicate: one row per (fingerprint, other_stripe_customer).
        # When the same card is found via multiple sources, keep the most direct one.
        if not df.empty and "match_source" in df.columns:
            _priority = {"stored_card": 0, "payment_method": 1, "charge": 2}
            df["_sort"] = df["match_source"].map(_priority).fillna(99)
            df = (df.sort_values("_sort")
                    .drop_duplicates(subset=["fingerprint", "other_stripe_customer"])
                    .drop(columns="_sort")
                    .reset_index(drop=True))

        flagged    = not df.empty
        n_prints   = df["fingerprint"].nunique()        if flagged else 0
        n_accounts = df["other_company_id"].nunique()   if flagged else 0
        msg = (
            f"{n_prints} fingerprint{'s' if n_prints != 1 else ''} matched across "
            f"{n_accounts} other account{'s' if n_accounts != 1 else ''}"
            if flagged else
            "No shared card fingerprints detected"
        )
        return _result("ALERT" if flagged else "CLEAR", msg, df, n_accounts)
    except Exception as exc:
        return _error(exc)


# ===========================================================================
# Account & Employee constants
# ===========================================================================
# Used by Signals 10–14.
# IMPORTANT: join order is locations → jobs → accounts (company filter first)
# to avoid full-scanning the large accounts table.
# Email/phone verification is on the accounts table itself:
#   Email verified : confirmed_at IS NOT NULL
#   Phone verified : needs_phone_confirmation = false

_ACCOUNTS_TABLE  = "prod_redshift_replica.postgres.accounts"
_JOBS_TABLE      = "prod_redshift_replica.postgres.jobs"
_LOCS_TABLE      = "prod_redshift_replica.postgres.locations"
_TIMECARDS_TABLE = "prod_redshift_replica.postgres.timecards"
_ONBOARD_DOCS    = "prod_redshift_replica.postgres.employee_onboarding_documents"

_FRAUD_DOMAINS_SQL = (
    "'mail.com', 'engineer.com', 'usa.com', 'consultant.com', 'myself.com', "
    "'dr.com', 'post.com', 'techie.com', 'writeme.com', 'cheerful.com'"
)


def _company_accounts_cte(company_id: int, extra_where: str = "") -> str:
    """
    CTE that resolves all active accounts for a company using a
    locations → jobs → accounts join order so the company_id filter
    runs first on the small locations table, not the large accounts table.
    extra_where: optional additional WHERE conditions on j.* or a.*
    """
    return f"""
    company_accounts AS (
        SELECT DISTINCT
            a.id                       AS account_id,
            a.email,
            a.phone,
            a.confirmed_at,
            a.needs_phone_confirmation,
            j.level                    AS user_role
        FROM {_LOCS_TABLE}     l
        JOIN {_JOBS_TABLE}     j ON j.location_id = l.id
        JOIN {_ACCOUNTS_TABLE} a ON a.id          = j.user_id
        WHERE l.company_id  = {company_id}
          AND j.archived_at IS NULL
          {extra_where}
    )"""


# ---------------------------------------------------------------------------
# Signal 10 — Suspicious Email Domains
# ---------------------------------------------------------------------------

def check_suspicious_email_domains(company_id: int) -> Dict[str, Any]:
    """
    ALERT if the owner or any employee is using an email domain
    historically associated with fraudulent or disposable accounts.
    """
    sql = f"""
    WITH {_company_accounts_cte(company_id)}
    SELECT DISTINCT
        ca.account_id                          AS user_id,
        ca.email,
        LOWER(SPLIT(ca.email, '@')[1])         AS email_domain,
        ca.user_role
    FROM company_accounts ca
    WHERE LOWER(SPLIT(ca.email, '@')[1]) IN ({_FRAUD_DOMAINS_SQL})
    ORDER BY ca.user_role, ca.email
    """
    try:
        df = run_query(sql)
        count = len(df)
        flagged = count > 0
        msg = (
            f"{count} account{'s' if count != 1 else ''} using a known fraud-associated email domain"
            if flagged else
            "No suspicious email domains detected"
        )
        return _result("ALERT" if flagged else "CLEAR", msg, df, count)
    except Exception as exc:
        return _error(exc)


# ---------------------------------------------------------------------------
# Signal 11 — Manager Email / Phone Verification
# ---------------------------------------------------------------------------

def check_owner_verification(company_id: int) -> Dict[str, Any]:
    """
    ALERT if any manager account has unverified email or phone.
    """
    sql = f"""
    WITH {_company_accounts_cte(company_id, "AND LOWER(j.level) = 'manager'")}
    SELECT
        ca.account_id,
        ca.email,
        ca.phone,
        ca.user_role,
        CASE WHEN ca.confirmed_at          IS NOT NULL THEN 'Verified' ELSE 'NOT VERIFIED' END AS email_verified,
        CASE WHEN ca.needs_phone_confirmation = false   THEN 'Verified' ELSE 'NOT VERIFIED' END AS phone_verified,
        ca.confirmed_at                                                                          AS email_verified_at
    FROM company_accounts ca
    WHERE ca.confirmed_at IS NULL OR ca.needs_phone_confirmation = true
    ORDER BY ca.account_id
    """
    try:
        df = run_query(sql)
        count = len(df)
        flagged = count > 0
        if flagged:
            parts = []
            if "email_verified" in df.columns and (df["email_verified"] == "NOT VERIFIED").any():
                parts.append("email unverified")
            if "phone_verified" in df.columns and (df["phone_verified"] == "NOT VERIFIED").any():
                parts.append("phone unverified")
            msg = f"{count} manager account{'s' if count != 1 else ''}: {' + '.join(parts) if parts else 'verification incomplete'}"
        else:
            msg = "All manager accounts have verified email and phone"
        return _result("ALERT" if flagged else "CLEAR", msg, df, count)
    except Exception as exc:
        return _error(exc)


# ---------------------------------------------------------------------------
# Signal 12 — Employee Email / Phone Verification
# ---------------------------------------------------------------------------

def check_employee_verification(company_id: int) -> Dict[str, Any]:
    """
    ALERT if zero non-manager employees have verified email or phone.
    Excludes accounts with no email AND no phone.
    """
    sql = f"""
    WITH {_company_accounts_cte(company_id, "AND LOWER(j.level) NOT IN ('owner', 'employer', 'manager')")}
    SELECT
        ca.account_id,
        ca.email,
        ca.phone,
        ca.user_role,
        CASE WHEN ca.confirmed_at          IS NOT NULL THEN 'Verified' ELSE 'NOT VERIFIED' END AS email_verified,
        CASE WHEN ca.needs_phone_confirmation = false   THEN 'Verified' ELSE 'NOT VERIFIED' END AS phone_verified,
        CASE WHEN ca.confirmed_at IS NOT NULL OR ca.needs_phone_confirmation = false
             THEN true ELSE false END                                                            AS any_verified
    FROM company_accounts ca
    WHERE ca.email IS NOT NULL OR ca.phone IS NOT NULL
    ORDER BY ca.user_role, ca.email
    """
    try:
        df = run_query(sql)
        if df.empty:
            return _result("PENDING", "No employee accounts with contact details found", df, 0)
        total    = len(df)
        verified = int(df["any_verified"].sum()) if "any_verified" in df.columns else 0
        flagged  = verified == 0
        msg = (
            f"No employees have verified their email or phone ({total} account{'s' if total != 1 else ''} checked)"
            if flagged else
            f"{verified} of {total} employee account{'s' if total != 1 else ''} have verified contact details"
        )
        return _result("ALERT" if flagged else "CLEAR", msg, df, verified)
    except Exception as exc:
        return _error(exc)


# ---------------------------------------------------------------------------
# Signal 13 — Suspicious Manager Timecard Overrides
# ---------------------------------------------------------------------------

def check_suspicious_timecards(company_id: int) -> Dict[str, Any]:
    """
    ALERT if a manager entered more than 3 timecard punches in the last 14 days.
    Uses a two-step approach to avoid a full timecards table scan:
      Step 1: fetch job IDs for this company (fast — small result)
      Step 2: query timecards WHERE job_id IN (...) using the explicit ID list
    Short-circuits immediately if the company has no active jobs.
    """
    THRESHOLD = 3

    # Step 1 — get job IDs for this company
    jobs_sql = f"""
    SELECT j.id AS job_id
    FROM {_LOCS_TABLE} l
    JOIN {_JOBS_TABLE} j ON j.location_id = l.id
    WHERE l.company_id = {company_id}
      AND j.archived_at IS NULL
    """
    try:
        jobs_df = run_query(jobs_sql)
        if jobs_df.empty:
            return _result("CLEAR", "No active jobs found for this company", pd.DataFrame(), 0)

        job_ids = ", ".join(str(jid) for jid in jobs_df["job_id"].tolist())

        # Step 2 — query timecards only for those specific job IDs
        tc_sql = f"""
        SELECT
            tc.id                       AS timecard_id,
            j.user_id,
            j.level                     AS user_role,
            tc.start_at,
            tc.end_at,
            tc.clock_in_source,
            tc.clock_out_source,
            tc.approved,
            DAYOFWEEK(tc.start_at)      AS day_of_week,
            HOUR(tc.start_at)           AS start_hour,
            HOUR(tc.end_at)             AS end_hour
        FROM {_TIMECARDS_TABLE} tc
        JOIN {_JOBS_TABLE} j ON tc.job_id = j.id
        WHERE tc.job_id IN ({job_ids})
          AND tc.clock_in_source = 'manager'
          AND tc.start_at >= CURRENT_DATE - INTERVAL 14 DAYS
          AND tc.start_at IS NOT NULL
          AND tc.end_at   IS NOT NULL
        ORDER BY tc.start_at DESC
        LIMIT 100
        """
        df    = run_query(tc_sql)
        count = len(df)
        flagged = count > THRESHOLD
        if flagged:
            msg = (
                f"{count} manager-entered punch{'es' if count != 1 else ''} in the last 14 days "
                f"— exceeds threshold of {THRESHOLD}"
            )
        elif count > 0:
            msg = f"{count} manager-entered punch{'es' if count != 1 else ''} in the last 14 days — within normal range"
        else:
            msg = "No manager-entered punches in the last 14 days"
        return _result("ALERT" if flagged else "CLEAR", msg, df if flagged else pd.DataFrame(), count)
    except Exception as exc:
        return _error(exc)


# ---------------------------------------------------------------------------
# Signal 14 — Employee Onboarding Documents on File
# ---------------------------------------------------------------------------

def check_employee_documents(company_id: int) -> Dict[str, Any]:
    """
    CLEAR (positive) if onboarding documents are on file — indicates a more established company.
    CLEAR (neutral)  if none found — not inherently bad, but provides no positive signal.
    """
    sql = f"""
    SELECT
        eod.id                            AS document_id,
        eod.user_id,
        eod.category,
        eod.filename,
        eod.uploaded_by_id,
        eod.acknowledged_at,
        CAST(eod.created_at AS TIMESTAMP) AS uploaded_at
    FROM {_ONBOARD_DOCS} eod
    WHERE eod.company_id = {company_id}
    ORDER BY eod.created_at DESC
    """
    try:
        df = run_query(sql)
        count = len(df)
        has_docs = count > 0
        msg = (
            f"{count} onboarding document{'s' if count != 1 else ''} on file — positive signal"
            if has_docs else
            "No onboarding documents found — no positive signal from document verification"
        )
        return _result("CLEAR", msg, df, count)
    except Exception as exc:
        return _error(exc)


# ---------------------------------------------------------------------------
# Signal 15 — Payment Method on File
# ---------------------------------------------------------------------------

def check_payment_method_on_file(company_id: int) -> Dict[str, Any]:
    """
    ALERT if no Stripe customer is found, or if no payment method is stored.

    Customer lookup uses metadata only (subscription + charge) — the email fallback
    was removed because one email can map to multiple Stripe customers, causing the
    wrong customer to be returned.

    Payment method presence is checked against customer_source (legacy stored cards,
    direct fingerprint column) and payment_method (PaymentMethod API, card JSON).
    This catches saved cards even before any charge has been made.
    """
    sql = f"""
    WITH company_customers AS (
        SELECT DISTINCT customer AS stripe_customer
        FROM {_SUB_TABLE}
        WHERE {_META_CO_ID} = '{company_id}'
          AND customer IS NOT NULL
        UNION
        SELECT DISTINCT customer AS stripe_customer
        FROM {_CHG_TABLE}
        WHERE {_META_CO_ID} = '{company_id}'
          AND customer IS NOT NULL
    ),
    stored_pms AS (
        SELECT cs.customer, cs.id AS pm_id, cs.fingerprint, 'customer_source' AS pm_type
        FROM {_CUST_SRC_TABLE} cs
        INNER JOIN company_customers cc ON cc.stripe_customer = cs.customer
        WHERE cs.row_deleted_at IS NULL
          AND cs.fingerprint IS NOT NULL

        UNION ALL

        SELECT pm.customer, pm.id AS pm_id,
               GET_JSON_OBJECT(pm.card, '$.fingerprint') AS fingerprint,
               'payment_method' AS pm_type
        FROM {_PM_TABLE} pm
        INNER JOIN company_customers cc ON cc.stripe_customer = pm.customer
        WHERE pm.row_deleted_at IS NULL
          AND GET_JSON_OBJECT(pm.card, '$.fingerprint') IS NOT NULL
    )
    SELECT
        cc.stripe_customer,
        COUNT(DISTINCT spm.fingerprint)                             AS payment_methods_count,
        COUNT(c.id)                                                 AS total_charges,
        SUM(CASE WHEN c.status = 'succeeded' THEN 1 ELSE 0 END)    AS successful_charges,
        SUM(CASE WHEN c.status = 'failed'    THEN 1 ELSE 0 END)    AS failed_charges,
        MIN(FROM_UNIXTIME(c.created))                               AS first_charge_at,
        MAX(FROM_UNIXTIME(c.created))                               AS last_charge_at
    FROM company_customers cc
    LEFT JOIN stored_pms spm ON spm.customer = cc.stripe_customer
    LEFT JOIN {_CHG_TABLE} c ON c.customer   = cc.stripe_customer
    GROUP BY cc.stripe_customer
    """
    try:
        df = run_query(sql)
        if df.empty:
            return _result(
                "ALERT",
                "No Stripe customer record found — no payment method on file",
                pd.DataFrame(),
                0,
            )
        pm_count   = int(df["payment_methods_count"].sum()) if "payment_methods_count" in df.columns else 0
        total      = int(df["total_charges"].sum())         if "total_charges"         in df.columns else 0
        successful = int(df["successful_charges"].sum())    if "successful_charges"    in df.columns else 0
        customer   = df["stripe_customer"].iloc[0]
        if pm_count > 0:
            msg = (
                f"Payment method on file ({customer})"
                + (f" — {total} charge{'s' if total != 1 else ''} ({successful} successful)" if total > 0 else "")
            )
            return _result("CLEAR", msg, df, pm_count)
        else:
            msg = f"Stripe customer found ({customer}) but no payment method on file"
            return _result("ALERT", msg, df, 0)
    except Exception as exc:
        return _error(exc)
