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

_HOST      = os.environ.get("DATABRICKS_HOST",         "").strip()
_HTTP_PATH = os.environ.get("SQL_WAREHOUSE_HTTP_PATH", "").strip()


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

    # credentials_provider must be a two-level callable:
    #   provider()  → returns header_factory
    #   header_factory()  → returns {"Authorization": "Bearer <token>"}
    # (HeaderFactory = Callable[[], Dict[str, str]] in databricks-sql-connector 3.1.0)
    def _provider():
        def _factory() -> dict:
            return {"Authorization": f"Bearer {token}"}
        return _factory

    conn = dbsql.connect(
        server_hostname=_HOST,
        http_path=_HTTP_PATH,
        credentials_provider=_provider,
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_text)
            rows = cursor.fetchall()
            cols = [d[0] for d in cursor.description] if cursor.description else []
            return pd.DataFrame(rows, columns=cols)
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
# TABLE NAME NOTES
# ─────────────────
# These two paths are from the original Redshift query.
# If the Databricks schema names differ, update the constants below —
# everything else will pick up the change automatically.
#
#   _IP_SIGNUP_TABLE   : Heap signup events with IP, city, region, country
#                        Original: prod_redshift_replica.heap.sign_up_owner_signed_up
#                        Databricks equivalent may be: heap.sign_up_owner_signed_up
#                        or ext_heap.sign_up_owner_signed_up
#
#   _LOCATION_TABLE    : Locations with provided city / state / zip
#                        Original: prod_raw.homebase1.locations
#                        Databricks equivalent may be: public.locations
#                        (verify that city, state, zip columns exist)
#
# HOW location_match_status IS SCORED
# ─────────────────────────────────────
#   'City Match'       →  ip_city == provided_city          (CLEAR)
#   'State Match Only' →  ip_state == provided_state only   (surfaced, not ALERTed)
#   'No Match'         →  neither city nor state matches    (ALERT)
#
# mismatch_pct heuristic (from the original query):
#   0%   exact city match
#   10%  partial city name overlap
#   25%  known CDN/cloud hub city with state mismatch (Ashburn, Atlanta, etc.)
#   30%  state match only
#   60%  US country match, city/state mismatch
#   65%  Canada, city/state mismatch
#   90%  foreign country
# ---------------------------------------------------------------------------

_IP_SIGNUP_TABLE = "prod_redshift_replica.heap.sign_up_owner_signed_up"
_LOCATION_TABLE  = "prod_redshift_replica.homebase1.locations"


def check_ip_location_mismatch(company_id: int) -> Dict[str, Any]:
    """
    ALERT if the IP address at account creation does not align with the
    company's registered city or state.

    Returns ALL rows (City Match, State Match Only, No Match) so the rep
    gets the full picture. ALERTs when at least one row is 'No Match'.
    mismatch_pct is the heuristic likelihood that this is genuine fraud
    (not a VPN / cloud IP / CDN exit node).
    """
    sql = f"""
    WITH state_abbrev AS (
        SELECT abbr, full_name
        FROM (VALUES
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
        SELECT
            company_id,
            location_id,
            ip,
            city        AS ip_city,
            region      AS ip_region,
            country     AS ip_country,
            time        AS signup_time
        FROM {_IP_SIGNUP_TABLE}
        WHERE ip         IS NOT NULL
          AND company_id  = {company_id}
    ),
    location_address AS (
        SELECT
            id           AS location_id,
            company_id,
            city         AS provided_city,
            state        AS provided_state,
            zip,
            address_1
        FROM {_LOCATION_TABLE}
        WHERE city        IS NOT NULL
          AND company_id  = {company_id}
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
        COALESCE(sa_ip.abbr,  LOWER(TRIM(sg.ip_region)))       AS ip_state_normalized,
        COALESCE(LOWER(sa_loc.full_name), LOWER(TRIM(la.provided_state))) AS provided_state_normalized,

        CASE
            WHEN LOWER(TRIM(sg.ip_city)) = LOWER(TRIM(la.provided_city))
                THEN 'City Match'
            WHEN COALESCE(LOWER(sa_ip.abbr),  LOWER(TRIM(sg.ip_region)))
               = COALESCE(LOWER(la.provided_state), '')
              OR LOWER(TRIM(sg.ip_region))
               = COALESCE(LOWER(sa_loc.full_name), '')
                THEN 'State Match Only'
            ELSE 'No Match'
        END AS location_match_status,

        CONCAT(
            CASE
                WHEN LOWER(TRIM(sg.ip_city)) = LOWER(TRIM(la.provided_city))
                    THEN '0'
                WHEN LOWER(la.provided_city) LIKE '%' || LOWER(TRIM(sg.ip_city)) || '%'
                  OR LOWER(TRIM(sg.ip_city)) LIKE '%' || LOWER(TRIM(la.provided_city)) || '%'
                    THEN '10'
                WHEN LOWER(TRIM(sg.ip_city)) IN (
                        'ashburn','atlanta','chicago','dallas',
                        'seattle','los angeles','san jose'
                     )
                  AND COALESCE(LOWER(sa_ip.abbr), LOWER(TRIM(sg.ip_region)))
                   != LOWER(TRIM(la.provided_state))
                  AND LOWER(TRIM(sg.ip_region))
                   != COALESCE(LOWER(sa_loc.full_name), '')
                    THEN '25'
                WHEN COALESCE(LOWER(sa_ip.abbr), LOWER(TRIM(sg.ip_region)))
                   = LOWER(TRIM(la.provided_state))
                  OR LOWER(TRIM(sg.ip_region)) = COALESCE(LOWER(sa_loc.full_name), '')
                    THEN '30'
                WHEN LOWER(TRIM(sg.ip_country)) = 'united states'     THEN '60'
                WHEN LOWER(TRIM(sg.ip_country)) = 'canada'            THEN '65'
                WHEN LOWER(TRIM(sg.ip_country))
                     NOT IN ('united states','canada')                 THEN '90'
                ELSE '50'
            END,
            '%'
        ) AS mismatch_pct,

        sg.signup_time
    FROM signup_geo sg
    JOIN location_address la
      ON sg.company_id = la.company_id
    LEFT JOIN state_abbrev sa_ip
      ON LOWER(TRIM(sg.ip_region)) = LOWER(sa_ip.full_name)
    LEFT JOIN state_abbrev sa_loc
      ON LOWER(TRIM(la.provided_state)) = LOWER(sa_loc.abbr)
    ORDER BY sg.signup_time DESC
    """
    try:
        df = run_query(sql)

        if df.empty:
            return _result(
                "CLEAR",
                "No signup geo data found for this company",
                df,
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
# Signal 5 — Dormancy Reactivation (30+ Day Gap)
# ---------------------------------------------------------------------------

def check_dormancy_reactivation(company_id: int) -> Dict[str, Any]:
    """
    ALERT if the company posted a job after a gap of 30+ days with no activity.
    created_at is a TIMESTAMP; gap is computed with DATEDIFF().
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
        created_at          AS resumed_posting_at,
        prev_created_at     AS last_post_before_gap,
        DATEDIFF(created_at, prev_created_at) AS gap_days,
        location_id,
        location_name
    FROM job_sequence
    WHERE prev_created_at IS NOT NULL
        AND DATEDIFF(created_at, prev_created_at) >= 30
    ORDER BY created_at DESC
    """
    try:
        df      = run_query(sql)
        count   = len(df)
        flagged = count > 0
        max_gap = int(df["gap_days"].max()) if flagged else 0
        msg = (
            f"{count} dormancy gap{'s' if count != 1 else ''} of 30+ days detected "
            f"(longest: {max_gap} days)"
            if flagged else
            "No dormancy-reactivation pattern detected"
        )
        return _result("ALERT" if flagged else "CLEAR", msg, df, count)
    except Exception as exc:
        return _error(exc)


# ===========================================================================
# Stripe constants — prod_redshift_replica.stripe.*
# ===========================================================================
# Tables confirmed from SHOW TABLES IN prod_redshift_replica.stripe
# Company linkage: GET_JSON_OBJECT(metadata, '$.company_id') on customer_subscription

_SUB_TABLE  = "prod_redshift_replica.stripe.customer_subscription"  # company → customer
_CHG_TABLE  = "prod_redshift_replica.stripe.charge"                 # Signals 6, 7, 8, 9
_CO_TABLE   = "prod_redshift_replica.public.companies"              # Signal 9 name lookup

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
    """ALERT if any Stripe charge for this company has been disputed."""
    sql = f"""
    WITH {_company_customers_cte(company_id)}
    SELECT
        c.id                            AS charge_id,
        ROUND(c.amount / 100.0, 2)      AS charge_amount_usd,
        UPPER(c.currency)               AS currency,
        c.status                        AS charge_status,
        FROM_UNIXTIME(c.created)        AS charged_at,
        c.customer                      AS stripe_customer
    FROM {_CHG_TABLE} c
    INNER JOIN company_customers cc ON cc.stripe_customer = c.customer
    WHERE c.disputed = true
    ORDER BY c.created DESC
    """
    try:
        df      = run_query(sql)
        count   = len(df)
        flagged = count > 0
        if flagged:
            total_usd = df["charge_amount_usd"].sum() if "charge_amount_usd" in df.columns else 0
            msg = f"{count} disputed charge{'s' if count != 1 else ''} (total: ${total_usd:,.2f})"
        else:
            msg = "No billing disputes found"
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
    ALERT if a card fingerprint on this company's charges also appears on
    charges from other companies — strong indicator of fraud ring activity.
    Fingerprint extracted from: GET_JSON_OBJECT(payment_method_details, '$.card.fingerprint')
    """
    sql = f"""
    WITH {_company_customers_cte(company_id)},
    target_fps AS (
        SELECT
            GET_JSON_OBJECT(c.payment_method_details, {_FINGERPRINT_PATH}) AS fingerprint,
            MIN(FROM_UNIXTIME(c.created)) AS first_used_on_this_account
        FROM {_CHG_TABLE} c
        INNER JOIN company_customers cc ON cc.stripe_customer = c.customer
        WHERE GET_JSON_OBJECT(c.payment_method_details, {_FINGERPRINT_PATH}) IS NOT NULL
        GROUP BY GET_JSON_OBJECT(c.payment_method_details, {_FINGERPRINT_PATH})
    ),
    other_matches AS (
        SELECT
            GET_JSON_OBJECT(c.payment_method_details, {_FINGERPRINT_PATH}) AS fingerprint,
            CAST({_META_CO_ID} AS INT)                                       AS other_company_id,
            MIN(FROM_UNIXTIME(c.created))                                    AS first_seen_at
        FROM {_CHG_TABLE} c
        INNER JOIN {_SUB_TABLE} s ON s.customer = c.customer
        INNER JOIN target_fps tf
          ON tf.fingerprint = GET_JSON_OBJECT(c.payment_method_details, {_FINGERPRINT_PATH})
        WHERE {_META_CO_ID} != '{company_id}'
          AND {_META_CO_ID} IS NOT NULL
          AND GET_JSON_OBJECT(c.payment_method_details, {_FINGERPRINT_PATH}) IS NOT NULL
        GROUP BY
            GET_JSON_OBJECT(c.payment_method_details, {_FINGERPRINT_PATH}),
            {_META_CO_ID}
    )
    SELECT
        om.fingerprint,
        om.other_company_id,
        co.name                          AS other_company_name,
        om.first_seen_at                 AS first_seen_at_other_account,
        tf.first_used_on_this_account
    FROM other_matches om
    INNER JOIN target_fps tf ON tf.fingerprint = om.fingerprint
    LEFT JOIN {_CO_TABLE} co ON co.company_id = om.other_company_id
    ORDER BY om.first_seen_at DESC
    """
    try:
        df         = run_query(sql)
        flagged    = not df.empty
        n_prints   = df["fingerprint"].nunique()      if flagged else 0
        n_accounts = df["other_company_id"].nunique() if flagged else 0
        msg = (
            f"{n_prints} fingerprint{'s' if n_prints != 1 else ''} matched across "
            f"{n_accounts} other account{'s' if n_accounts != 1 else ''}"
            if flagged else
            "No shared card fingerprints detected"
        )
        return _result("ALERT" if flagged else "CLEAR", msg, df, n_accounts)
    except Exception as exc:
        return _error(exc)
