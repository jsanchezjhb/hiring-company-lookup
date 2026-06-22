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

Table notes (Databricks / Spark SQL):
  - hiring_job_requests.created_at  → microsecond epoch  → TIMESTAMP_MICROS()
  - hiring_job_requests.activated_at → regular timestamp, no conversion needed
  - Always filter hiring_version = 2
  - Always exclude test company 1987234
  - Job scope: company_id → public.locations → postgres.hiring_job_requests
"""

from __future__ import annotations

from typing import Any, Dict
import pandas as pd
from db_utils import run_query


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

EXCLUDE_TEST_COMPANY = "AND c.company_id != 1987234"

# Microsecond constants
_MICROS_PER_SECOND = 1_000_000
_MICROS_PER_MINUTE = 60 * _MICROS_PER_SECOND
_MICROS_PER_HOUR   = 60 * _MICROS_PER_MINUTE
_MICROS_PER_DAY    = 24 * _MICROS_PER_HOUR  # 86_400_000_000


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
        CAST(TIMESTAMP_MICROS(hjr.created_at) AS DATE)  AS created_date,
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
    less than 60 seconds apart — a pattern consistent with automation or bulk fraud.

    Method: LAG() over the raw microsecond created_at to avoid timestamp
    conversion in the WHERE clause. 1 minute = 60,000,000 microseconds.
    """
    THRESHOLD_MICROS = 60 * _MICROS_PER_SECOND  # 60_000_000

    sql = f"""
    WITH job_sequence AS (
        SELECT
            hjr.id                                         AS job_post_id,
            hjr.title                                      AS job_title,
            hjr.status,
            hjr.created_at                                 AS created_at_micros,
            TIMESTAMP_MICROS(hjr.created_at)               AS created_at,
            LAG(hjr.created_at) OVER (
                PARTITION BY c.company_id
                ORDER BY hjr.created_at ASC
            )                                              AS prev_created_at_micros,
            l.location_id,
            l.name                                         AS location_name
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
        TIMESTAMP_MICROS(prev_created_at_micros)            AS prev_job_created_at,
        CAST(
            (created_at_micros - prev_created_at_micros) / {_MICROS_PER_SECOND}
        AS INT)                                             AS seconds_between_posts,
        ROUND(
            (created_at_micros - prev_created_at_micros) / {_MICROS_PER_MINUTE}.0,
        2)                                                  AS minutes_between_posts,
        location_id,
        location_name
    FROM job_sequence
    WHERE prev_created_at_micros IS NOT NULL
        AND (created_at_micros - prev_created_at_micros) < {THRESHOLD_MICROS}
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
            TIMESTAMP_MICROS(hjr.created_at)                   AS created_at,
            DATE_TRUNC('HOUR', TIMESTAMP_MICROS(hjr.created_at)) AS hour_bucket,
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
_LOCATION_TABLE  = "prod_raw.homebase1.locations"


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
    ALERT if the company posted a job after a gap of 30+ days with no
    posting activity — a pattern consistent with account takeover or
    a reactivated fraud ring.

    Method: LAG() on raw microsecond created_at.
    30 days = 2,592,000,000,000 microseconds.
    """
    THRESHOLD_MICROS = 30 * _MICROS_PER_DAY  # 2_592_000_000_000

    sql = f"""
    WITH job_sequence AS (
        SELECT
            hjr.id                                         AS job_post_id,
            hjr.title                                      AS job_title,
            hjr.status,
            hjr.created_at                                 AS created_at_micros,
            TIMESTAMP_MICROS(hjr.created_at)               AS created_at,
            LAG(hjr.created_at) OVER (
                PARTITION BY c.company_id
                ORDER BY hjr.created_at ASC
            )                                              AS prev_created_at_micros,
            l.location_id,
            l.name                                         AS location_name
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
        created_at                                          AS resumed_posting_at,
        TIMESTAMP_MICROS(prev_created_at_micros)            AS last_post_before_gap,
        CAST(
            (created_at_micros - prev_created_at_micros) / {_MICROS_PER_DAY}
        AS INT)                                             AS gap_days,
        location_id,
        location_name
    FROM job_sequence
    WHERE prev_created_at_micros IS NOT NULL
        AND (created_at_micros - prev_created_at_micros) >= {THRESHOLD_MICROS}
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


# ---------------------------------------------------------------------------
# Signal 6 — Failed / Unsuccessful Billing  (PENDING — needs table names)
# ---------------------------------------------------------------------------

def check_failed_billing(company_id: int) -> Dict[str, Any]:
    """
    ALERT if the company has any unsuccessful billing attempts from Stripe.

    STATUS: Pending — need to confirm the Stripe billing table name
    (e.g. postgres.stripe_charges, postgres.biller_charges, ext_stripe.charges).
    """
    return _pending(
        "Awaiting Stripe table confirmation. "
        "Please share the table name for failed billing attempts "
        "(e.g. postgres.stripe_charges or a similar table)."
    )


# ---------------------------------------------------------------------------
# Signal 7 — Billing Disputes  (PENDING — needs table names)
# ---------------------------------------------------------------------------

def check_billing_disputes(company_id: int) -> Dict[str, Any]:
    """
    ALERT if the company has any open or resolved billing disputes.

    STATUS: Pending — need the Stripe disputes table name
    (e.g. postgres.stripe_disputes, ext_stripe.disputes).
    """
    return _pending(
        "Awaiting Stripe disputes table confirmation. "
        "Please share the table name for billing disputes."
    )


# ---------------------------------------------------------------------------
# Signal 8 — Excessive Payment Method Changes  (PENDING — needs table names)
# ---------------------------------------------------------------------------

def check_payment_method_changes(company_id: int) -> Dict[str, Any]:
    """
    ALERT if the company has changed their payment method more than 2 times —
    may indicate card testing or identity fraud.

    STATUS: Pending — need the payment methods change-log table name
    (e.g. postgres.stripe_payment_methods, postgres.biller_payment_methods).
    """
    return _pending(
        "Awaiting Stripe payment methods table confirmation. "
        "Please share the table name for payment method history."
    )


# ---------------------------------------------------------------------------
# Signal 9 — Stripe Fingerprint Reuse Across Companies
# ---------------------------------------------------------------------------
#
# HOW TO ACTIVATE THIS SIGNAL
# ────────────────────────────
# 1. Confirm the table and column names:
#      TABLE   : e.g.  postgres.stripe_payment_methods  OR  postgres.biller_payment_methods
#      COLUMNS : fingerprint  (Stripe card fingerprint, same value for the same card number
#                              regardless of expiry or CVV changes)
#                company_id   (or an owner_id that maps to a company via locations)
#                created_at
#
# 2. Replace both _STRIPE_PM_TABLE placeholder strings below with the real table name.
# 3. If the table links to locations instead of companies directly, adjust the join
#    pattern to go through public.locations first (see the commented-out variant).
# 4. Delete the _pending() return and uncomment the try/except block.
#
# ---------------------------------------------------------------------------
#
# FULL QUERY (ready to activate):
#
# WITH target_fingerprints AS (
#     -- Collect every Stripe card fingerprint on file for this company
#     SELECT DISTINCT
#         spm.fingerprint,
#         spm.created_at AS added_to_this_account_at
#     FROM _STRIPE_PM_TABLE spm
#     WHERE spm.company_id = {company_id}          -- adjust if joined via location
#       AND spm.fingerprint IS NOT NULL
# ),
# shared_on_other_accounts AS (
#     -- Find every OTHER company that has ever used the same fingerprint
#     SELECT
#         spm.fingerprint,
#         c.company_id    AS other_company_id,
#         c.name          AS other_company_name,
#         MIN(CAST(spm.created_at AS TIMESTAMP)) AS first_seen_at_other_account
#     FROM _STRIPE_PM_TABLE spm
#     INNER JOIN public.companies c ON c.company_id = spm.company_id
#     INNER JOIN target_fingerprints tf ON tf.fingerprint = spm.fingerprint
#     WHERE spm.company_id != {company_id}
#       AND c.company_id    != 1987234
#     GROUP BY spm.fingerprint, c.company_id, c.name
# )
# SELECT
#     s.fingerprint,
#     s.other_company_id,
#     s.other_company_name,
#     s.first_seen_at_other_account,
#     tf.added_to_this_account_at
# FROM shared_on_other_accounts s
# INNER JOIN target_fingerprints tf ON tf.fingerprint = s.fingerprint
# ORDER BY s.first_seen_at_other_account DESC
#
# ---------------------------------------------------------------------------

_STRIPE_PM_TABLE = "PENDING_TABLE_NAME"  # ← replace with real table name


def check_fingerprint_reuse(company_id: int) -> Dict[str, Any]:
    """
    ALERT if any Stripe card fingerprint on this company's account also appears
    on one or more OTHER company accounts.

    This is a cross-company signal — the same physical card (same fingerprint)
    being used to pay for multiple separate accounts is a strong indicator of
    a fraud ring, bulk account creation, or account takeover.

    Note: Stripe fingerprints are stable across card re-issues with new expiry
    dates, so a match here is highly reliable.

    STATUS: Pending — awaiting Stripe payment methods table name and column layout.
    """
    if _STRIPE_PM_TABLE == "PENDING_TABLE_NAME":
        return _pending(
            "Awaiting Stripe payment methods table name. "
            "Please confirm the table that stores Stripe card fingerprints "
            "(e.g. postgres.stripe_payment_methods or postgres.biller_payment_methods) "
            "and whether it links to company_id directly or via location_id."
        )

    # ── Activated once table name is set above ────────────────────────────
    sql = f"""
    WITH target_fingerprints AS (
        SELECT DISTINCT
            spm.fingerprint,
            CAST(spm.created_at AS TIMESTAMP) AS added_to_this_account_at
        FROM {_STRIPE_PM_TABLE} spm
        WHERE spm.company_id   = {company_id}
          AND spm.fingerprint IS NOT NULL
    ),
    shared_on_other_accounts AS (
        SELECT
            spm.fingerprint,
            c.company_id  AS other_company_id,
            c.name        AS other_company_name,
            MIN(CAST(spm.created_at AS TIMESTAMP)) AS first_seen_at_other_account
        FROM {_STRIPE_PM_TABLE} spm
        INNER JOIN public.companies c ON c.company_id = spm.company_id
        INNER JOIN target_fingerprints tf ON tf.fingerprint = spm.fingerprint
        WHERE spm.company_id != {company_id}
          AND c.company_id    != 1987234
        GROUP BY spm.fingerprint, c.company_id, c.name
    )
    SELECT
        s.fingerprint,
        s.other_company_id,
        s.other_company_name,
        s.first_seen_at_other_account,
        tf.added_to_this_account_at
    FROM shared_on_other_accounts s
    INNER JOIN target_fingerprints tf ON tf.fingerprint = s.fingerprint
    ORDER BY s.first_seen_at_other_account DESC
    """
    try:
        df      = run_query(sql)
        count   = len(df)
        flagged = count > 0

        # Unique fingerprints and companies in the result for the summary message
        n_prints   = df["fingerprint"].nunique()       if flagged else 0
        n_accounts = df["other_company_id"].nunique()  if flagged else 0

        msg = (
            f"{n_prints} fingerprint{'s' if n_prints != 1 else ''} matched on "
            f"{n_accounts} other company account{'s' if n_accounts != 1 else ''}"
            if flagged else
            "No shared card fingerprints detected across other accounts"
        )
        return _result("ALERT" if flagged else "CLEAR", msg, df, n_accounts)
    except Exception as exc:
        return _error(exc)
