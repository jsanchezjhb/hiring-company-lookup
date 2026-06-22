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


# ===========================================================================
# Stripe table / column constants
# ===========================================================================
# All Stripe signal functions reference these constants.
# Update a constant here → every query that uses it picks up the change.
#
# Tables confirmed from workspace screenshot:
#   prod_enriched.stripe.stripe_transactions  — tx-level: amount, status, invoices, subscriptions
#   prod_enriched.stripe.stripe_subscription  — subscription details incl. company/location
#   prod_raw.stripe.charge                    — raw charge objects (disputed flag, fingerprint)
#
# Columns marked ✅ confirmed from SELECT * … LIMIT 1 output.
# Columns marked ← VERIFY still need a quick check.
# ===========================================================================

# Tables
_TXN_TABLE  = "prod_enriched.stripe.stripe_transactions"   # Signal 6
_SUB_TABLE  = "prod_enriched.stripe.stripe_subscription"   # Signals 7, 8, 9 (company linkage)
_CHG_TABLE  = "prod_raw.stripe.charge"                     # Signals 7, 8, 9

# stripe_subscription columns — ✅ confirmed
_SUB_COMPANY_COL   = "company_id"          # ✅ confirmed
_SUB_CUSTOMER_COL  = "biller_customer_id"  # ✅ confirmed (holds cus_* Stripe ID)

# prod_raw.stripe.charge columns — ✅ confirmed from charge LIMIT 1 output
_CHARGE_CUSTOMER_COL = "customer"          # ✅ confirmed (holds cus_* Stripe ID)
_CHARGE_PM_COL       = "payment_method"    # ✅ confirmed (holds pm_* Stripe ID)

# Fingerprint lives inside the payment_method_details JSON column, not as a
# flat column. Confirmed structure from charge schema:
#   payment_method_details → {"card": {"fingerprint": "YJr2g3I1oY2K5Nms", ...}}
# Signal 9 uses GET_JSON_OBJECT(c.payment_method_details, _FINGERPRINT_PATH)
_FINGERPRINT_PATH = "'$.card.fingerprint'"  # ✅ confirmed path

# charge.created is a Unix epoch in SECONDS (e.g. 1659579357).
# All charge queries use FROM_UNIXTIME(c.created) — NOT CAST(... AS TIMESTAMP).
# (Subscription/transaction tables use ISO timestamps, so those stay as CAST.)

# stripe_transactions columns — ✅ confirmed from stripe_transactions LIMIT 1 output
_TXN_COMPANY_COL    = "company_id"      # ✅ confirmed
# Status column is status_name (not status). Confirmed values include 'paid'.
# Non-paid rows are flagged as unsuccessful billing attempts.
# ← If you want to narrow to a specific failed status (e.g. 'failed', 'open'),
#   update the WHERE clause in check_failed_billing directly.

# Alert threshold for payment method changes (Signal 8).
_PM_THRESHOLD = 2                       # alert when distinct methods > this


# ---------------------------------------------------------------------------
# Signal 6 — Failed / Unsuccessful Billing
# ---------------------------------------------------------------------------
#
# Confirmed stripe_transactions columns:
#   status_name     — 'paid' = success; anything else = unsuccessful
#   transaction_at  — already an ISO timestamp, no conversion needed
#   transaction_id  — Stripe event ID (evt_*)
#   invoice_id      — Stripe invoice ID (in_*)
#   subscription_uuid — internal subscription UUID
#   company_id      — direct company filter ✅
#   amount          — ← VERIFY units: enriched tables sometimes pre-convert
#                       cents → dollars. Sample row showed 0 so units unclear.
#                       If amounts look 100x too high, divide by 100 here.
# ---------------------------------------------------------------------------

def check_failed_billing(company_id: int) -> Dict[str, Any]:
    """
    ALERT if the company has any transactions in stripe_transactions where
    status_name != 'paid' — indicating an unsuccessful billing attempt.

    transaction_at is already a proper timestamp (no conversion needed).
    """
    sql = f"""
    SELECT
        t.transaction_id,
        t.invoice_id,
        t.subscription_uuid,
        t.status_name,
        t.amount,                       -- ← VERIFY units (may already be dollars)
        UPPER(t.currency)               AS currency,
        t.sales_tax,
        t.discount_applied,
        t.transaction_at                AS charged_at
    FROM {_TXN_TABLE} t
    WHERE t.{_TXN_COMPANY_COL} = {company_id}
      AND t.status_name != 'paid'
    ORDER BY t.transaction_at DESC
    """
    try:
        df      = run_query(sql)
        count   = len(df)
        flagged = count > 0

        if flagged:
            # Group by distinct status_name values for the summary line
            status_breakdown = (
                df["status_name"].value_counts().to_dict()
                if "status_name" in df.columns else {}
            )
            breakdown_str = "  |  ".join(
                f"{v} '{k}'" for k, v in status_breakdown.items()
            )
            msg = f"{count} unsuccessful billing transaction{'s' if count != 1 else ''}: {breakdown_str}"
        else:
            msg = "No unsuccessful billing transactions found"

        return _result("ALERT" if flagged else "CLEAR", msg, df, count)
    except Exception as exc:
        return _error(exc)


# ---------------------------------------------------------------------------
# Signal 7 — Billing Disputes
# ---------------------------------------------------------------------------
#
# charge.disputed (boolean) ✅ confirmed in schema.
# No amount_disputed column exists — we show the full charge amount.
# charge.created is Unix seconds → FROM_UNIXTIME().
# ---------------------------------------------------------------------------

def check_billing_disputes(company_id: int) -> Dict[str, Any]:
    """
    ALERT if any Stripe charge linked to this company has been disputed.

    Join path: company_id → stripe_subscription.biller_customer_id → charge.customer
    charge.disputed confirmed as a boolean column in the charge schema.
    """
    sql = f"""
    WITH company_customers AS (
        SELECT DISTINCT {_SUB_CUSTOMER_COL} AS stripe_customer
        FROM {_SUB_TABLE}
        WHERE {_SUB_COMPANY_COL} = {company_id}
          AND {_SUB_CUSTOMER_COL} IS NOT NULL
    )
    SELECT
        c.id                                    AS charge_id,
        ROUND(c.amount / 100.0, 2)              AS charge_amount_usd,
        UPPER(c.currency)                       AS currency,
        c.status                                AS charge_status,
        COALESCE(c.failure_code,    'n/a')      AS failure_code,
        COALESCE(c.failure_message, 'n/a')      AS failure_message,
        FROM_UNIXTIME(c.created)                AS charged_at,
        c.{_CHARGE_CUSTOMER_COL}                AS stripe_customer
    FROM {_CHG_TABLE} c
    INNER JOIN company_customers cc
      ON cc.stripe_customer = c.{_CHARGE_CUSTOMER_COL}
    WHERE c.disputed = true
    ORDER BY c.created DESC
    """
    try:
        df      = run_query(sql)
        count   = len(df)
        flagged = count > 0

        if flagged:
            total_usd = df["charge_amount_usd"].sum() if "charge_amount_usd" in df.columns else 0
            msg = (
                f"{count} disputed charge{'s' if count != 1 else ''} "
                f"(total charge value: ${total_usd:,.2f})"
            )
        else:
            msg = "No billing disputes found"

        return _result("ALERT" if flagged else "CLEAR", msg, df, count)
    except Exception as exc:
        return _error(exc)


# ---------------------------------------------------------------------------
# Signal 8 — Excessive Payment Method Changes
# ---------------------------------------------------------------------------
#
# stripe_subscription does not carry a payment method column, so we derive
# distinct payment methods from prod_raw.stripe.charge instead.
#
# Join path: company_id → stripe_subscription.biller_customer_id
#                       → charge.customer → charge.payment_method
#
# Each unique payment_method ID represents a different card or bank account.
# More than _PM_THRESHOLD distinct values triggers an ALERT.
# ---------------------------------------------------------------------------

def check_payment_method_changes(company_id: int) -> Dict[str, Any]:
    """
    ALERT if the company has used more than 2 distinct Stripe payment methods
    across all of their charges — may indicate card testing or fraud.

    Shows one row per distinct payment method with usage count and date range.
    """
    sql = f"""
    WITH company_customers AS (
        SELECT DISTINCT {_SUB_CUSTOMER_COL} AS stripe_customer
        FROM {_SUB_TABLE}
        WHERE {_SUB_COMPANY_COL} = {company_id}
          AND {_SUB_CUSTOMER_COL} IS NOT NULL
    )
    SELECT
        c.{_CHARGE_PM_COL}                      AS payment_method_id,
        COUNT(*)                                 AS times_charged,
        ROUND(SUM(c.amount) / 100.0, 2)          AS total_charged_usd,
        MIN(FROM_UNIXTIME(c.created))            AS first_used_at,
        MAX(FROM_UNIXTIME(c.created))            AS last_used_at
    FROM {_CHG_TABLE} c
    INNER JOIN company_customers cc
      ON cc.stripe_customer = c.{_CHARGE_CUSTOMER_COL}
    WHERE c.{_CHARGE_PM_COL} IS NOT NULL
    GROUP BY c.{_CHARGE_PM_COL}
    ORDER BY first_used_at DESC
    """
    try:
        df = run_query(sql)

        if df.empty:
            return _result("CLEAR", "No charge payment method history found", df, 0)

        distinct_pms = len(df)
        flagged      = distinct_pms > _PM_THRESHOLD

        msg = (
            f"{distinct_pms} distinct payment methods on record — "
            f"exceeds threshold of {_PM_THRESHOLD}"
            if flagged else
            f"{distinct_pms} distinct payment method{'s' if distinct_pms != 1 else ''} — within normal range"
        )
        return _result("ALERT" if flagged else "CLEAR", msg, df, distinct_pms)
    except Exception as exc:
        return _error(exc)


# ---------------------------------------------------------------------------
# Signal 9 — Stripe Card Fingerprint Reuse Across Companies
# ---------------------------------------------------------------------------
#
# Fingerprint confirmed as nested JSON (not a flat column):
#   payment_method_details → {"card": {"fingerprint": "YJr2g3I1oY2K5Nms", ...}}
#   Extracted with: GET_JSON_OBJECT(c.payment_method_details, '$.card.fingerprint')
#
# charge.created is Unix seconds → FROM_UNIXTIME().
#
# Join path for both sides of the cross-company check:
#   company_id  →  stripe_subscription.biller_customer_id
#              →  charge.customer
#              →  GET_JSON_OBJECT(charge.payment_method_details, '$.card.fingerprint')
# ---------------------------------------------------------------------------

def check_fingerprint_reuse(company_id: int) -> Dict[str, Any]:
    """
    ALERT if any Stripe card fingerprint used by this company also appears on
    charges belonging to one or more OTHER companies.

    The same physical card funding multiple separate accounts is a strong
    indicator of a fraud ring or bulk account creation. Fingerprints are
    stable across card re-issues (new expiry / CVV), so a match is highly reliable.

    Fingerprint is extracted from the payment_method_details JSON column:
      GET_JSON_OBJECT(payment_method_details, '$.card.fingerprint')
    """
    sql = f"""
    WITH target_customers AS (
        -- Stripe customer IDs belonging to the target company
        SELECT DISTINCT {_SUB_CUSTOMER_COL} AS stripe_customer
        FROM {_SUB_TABLE}
        WHERE {_SUB_COMPANY_COL} = {company_id}
          AND {_SUB_CUSTOMER_COL} IS NOT NULL
    ),
    target_charge_fps AS (
        -- Extract fingerprint from JSON for all charges on this company
        SELECT
            GET_JSON_OBJECT(c.payment_method_details, {_FINGERPRINT_PATH}) AS fingerprint,
            c.{_CHARGE_CUSTOMER_COL}                                         AS stripe_customer,
            c.created
        FROM {_CHG_TABLE} c
        INNER JOIN target_customers tc
          ON tc.stripe_customer = c.{_CHARGE_CUSTOMER_COL}
        WHERE GET_JSON_OBJECT(c.payment_method_details, {_FINGERPRINT_PATH}) IS NOT NULL
    ),
    target_fingerprints AS (
        SELECT
            fingerprint,
            MIN(FROM_UNIXTIME(created)) AS first_used_on_this_account
        FROM target_charge_fps
        GROUP BY fingerprint
    ),
    other_company_customers AS (
        -- Stripe customer IDs for all OTHER companies
        SELECT DISTINCT
            s.{_SUB_CUSTOMER_COL} AS stripe_customer,
            s.{_SUB_COMPANY_COL}  AS other_company_id
        FROM {_SUB_TABLE} s
        WHERE s.{_SUB_COMPANY_COL} != {company_id}
          AND s.{_SUB_COMPANY_COL} != 1987234
          AND s.{_SUB_CUSTOMER_COL} IS NOT NULL
    ),
    other_charge_fps AS (
        -- Extract fingerprint from JSON for charges on other companies
        SELECT
            GET_JSON_OBJECT(c.payment_method_details, {_FINGERPRINT_PATH}) AS fingerprint,
            occ.other_company_id,
            c.created
        FROM {_CHG_TABLE} c
        INNER JOIN other_company_customers occ
          ON occ.stripe_customer = c.{_CHARGE_CUSTOMER_COL}
        INNER JOIN target_fingerprints tf
          ON tf.fingerprint = GET_JSON_OBJECT(c.payment_method_details, {_FINGERPRINT_PATH})
        WHERE GET_JSON_OBJECT(c.payment_method_details, {_FINGERPRINT_PATH}) IS NOT NULL
    ),
    matched_other_accounts AS (
        SELECT
            ocf.fingerprint,
            ocf.other_company_id,
            co.name                       AS other_company_name,
            MIN(FROM_UNIXTIME(ocf.created)) AS first_seen_at_other_account
        FROM other_charge_fps ocf
        INNER JOIN public.companies co ON co.company_id = ocf.other_company_id
        GROUP BY ocf.fingerprint, ocf.other_company_id, co.name
    )
    SELECT
        m.fingerprint,
        m.other_company_id,
        m.other_company_name,
        m.first_seen_at_other_account,
        tf.first_used_on_this_account
    FROM matched_other_accounts m
    INNER JOIN target_fingerprints tf ON tf.fingerprint = m.fingerprint
    ORDER BY m.first_seen_at_other_account DESC
    """
    try:
        df      = run_query(sql)
        count   = len(df)
        flagged = count > 0

        n_prints   = df["fingerprint"].nunique()      if flagged else 0
        n_accounts = df["other_company_id"].nunique() if flagged else 0

        msg = (
            f"{n_prints} card fingerprint{'s' if n_prints != 1 else ''} matched across "
            f"{n_accounts} other company account{'s' if n_accounts != 1 else ''}"
            if flagged else
            "No shared card fingerprints detected across other accounts"
        )
        return _result("ALERT" if flagged else "CLEAR", msg, df, n_accounts)
    except Exception as exc:
        return _error(exc)
