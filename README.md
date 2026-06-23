# Hiring Fraud Detection Tool

Internal Streamlit app for the CS/Service team to investigate companies for fraud signals before or after they begin using Homebase's Hiring product. Enter a Company ID and run all 15 checks in one click.

**Live app:** `https://hiring-company-lookup-373323366197249.aws.databricksapps.com`
**Warehouse:** `readers-staging-sqlwh-01`
**Catalog:** `prod_redshift_replica`

---

## How to Use

1. Open the app and enter a **Company ID** on the home screen
2. Click **Run Analysis**
3. Each signal card shows **ALERT** (red), **CLEAR** (green), or **PENDING** (grey)
4. Expand any card to see the full detail table
5. Use the **Export CSV** button on any table to pull the raw data

---

## Signal Reference

Signals are grouped into four sections, displayed in this order:

### 🌐 Account & Identity

| # | Signal | ALERT when |
|---|--------|------------|
| 1 | **IP / Location Mismatch** | Signup IP doesn't match the company's registered city or state. Pulls from both Heap (pre-2023) and Amplitude via `dbt_staging.s_amp_owner_signups_raw` (post-2023). Includes a heuristic `mismatch_pct` score (CDN cities like Ashburn score lower than a foreign IP). |

---

### 👤 Account & Employee Risk

| # | Signal | ALERT when |
|---|--------|------------|
| 2 | **Suspicious Email Domains** | Owner or any employee is using a domain historically associated with fraud: `mail.com`, `engineer.com`, `usa.com`, `consultant.com`, `myself.com`, `dr.com`, `post.com`, `techie.com`, `writeme.com`, `cheerful.com` |
| 3 | **Manager Email / Phone Verification** | Any manager account (`jobs.level = 'Manager'`) has unverified email (`confirmed_at IS NULL`) or unverified phone (`needs_phone_confirmation = true`) |
| 4 | **Employee Email / Phone Verification** | Zero non-manager employees with contact details have verified email or phone. Shows total verified vs. total checked. Excludes accounts with no email AND no phone. |
| 5 | **Suspicious Manager Timecard Overrides** | A manager entered more than 3 timecard punches (`clock_in_source = 'manager'`) in the last 14 days (proxy for one pay period). Employees are expected to clock themselves in — frequent manager overrides may indicate fabricated records. |
| 6 | **Employee Onboarding Documents** | CLEAR (positive) when documents are on file; CLEAR (neutral, no positive signal) when none found. A document count of 0 is not inherently suspicious but doesn't help establish legitimacy. |

---

### 💳 Billing & Payments

| # | Signal | ALERT when |
|---|--------|------------|
| 7 | **Payment Method on File** | No Stripe customer found, OR customer exists but `default_source` is null (no payment method stored). Uses a three-way lookup: subscription metadata → charge metadata → manager email matched against `stripe.customer`. |
| 8 | **Failed Billing Attempts** | One or more failed Stripe charges on record |
| 9 | **Billing Disputes** | Any open or resolved Stripe disputes (chargebacks) |
| 10 | **Excessive Payment Method Changes** | More than 2 payment method changes on Stripe |
| 11 | **Stripe Fingerprint Reuse** | The company's card fingerprint appears on one or more other company accounts — strong indicator of a fraud ring |

---

### 📌 Job Posting Behaviour

| # | Signal | ALERT when |
|---|--------|------------|
| 12 | **Active Job Posts** | 10 or more currently active job postings |
| 13 | **Rapid Posting** | Any two consecutive jobs created less than 60 seconds apart |
| 14 | **Hourly Posting Burst** | 4 or more jobs posted within the same clock hour |
| 15 | **Dormancy Reactivation** | 30+ day gap in job posting activity followed by a sudden burst |

---

## Key Tables

### Hiring / Job Data
| Table | Purpose |
|-------|---------|
| `prod_redshift_replica.postgres.hiring_job_requests` | Job postings — `created_at` is already a TIMESTAMP |
| `prod_redshift_replica.postgres.accounts` | User accounts — email, phone, `confirmed_at`, `needs_phone_confirmation` |
| `prod_redshift_replica.postgres.jobs` | Employee↔Location link — `user_id`, `location_id`, `level` (title-cased: `'Manager'`, `'Employee'`) |
| `prod_redshift_replica.postgres.locations` | Location→Company link — `id`, `company_id`, `city`, `state`, `zip` |
| `prod_redshift_replica.postgres.timecards` | Punch data — `clock_in_source`, `start_at`, `end_at` |
| `prod_redshift_replica.postgres.employee_onboarding_documents` | Employee document uploads — `user_id`, `company_id`, `category`, `filename` |

### Signup Geo (IP Mismatch)
| Table | Purpose |
|-------|---------|
| `prod_redshift_replica.heap.sign_up_owner_signed_up` | Signup IP data pre-2023 (Heap) |
| `prod_redshift_replica.dbt_staging.s_amp_owner_signups_raw` | Signup IP data post-2023 (Amplitude workaround — `prod_enriched.amplitude` is restricted) |

### Stripe
| Table | Purpose |
|-------|---------|
| `prod_redshift_replica.stripe.customer_subscription` | Company→Stripe customer link via `GET_JSON_OBJECT(metadata, '$.company_id')` |
| `prod_redshift_replica.stripe.charge` | Charge history — `status`, `created` (Unix epoch), `customer` |
| `prod_redshift_replica.stripe.customer` | Stripe customer record — `email`, `default_source` (payment method presence) |

---

## Architecture

```
app.py          — Streamlit UI, signal cards, section layout
queries.py      — All SQL + result logic; one function per signal
app.yaml        — Required by Databricks Apps: command: [streamlit, run, app.py]
requirements.txt
```

### Auth Pattern (critical)
The app uses OAuth M2M via the Databricks SDK. **User Authorization must be OFF** in the app settings — enabling it injects `DATABRICKS_TOKEN` alongside the OAuth credentials, causing a "two auth methods" conflict.

```python
from databricks.sdk.core import Config
from databricks import sql as dbsql

cfg  = Config()   # picks up DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET
conn = dbsql.connect(
    server_hostname=DATABRICKS_HOST,
    http_path=DATABRICKS_HTTP_PATH,
    credentials_provider=lambda: cfg.authenticate,
)
```

SDK versions that work: `databricks-sdk==0.20.0` + `databricks-sql-connector==3.1.0`

### Global Data Quality
`run_query()` applies two transforms to every result before it reaches any signal:
1. **Deduplication** — `df.drop_duplicates()` so duplicate rows never inflate counts or clutter tables
2. **ID formatting** — any column named `id` or ending in `_id` is cast to string, preventing Streamlit from rendering IDs with comma separators (e.g. `1,234,567` → `1234567`)

---

## Adding a New Signal

1. **`queries.py`** — add a function following the pattern:
```python
def check_my_signal(company_id: int) -> Dict[str, Any]:
    sql = f"""SELECT ... FROM ... WHERE company_id = {company_id}"""
    try:
        df    = run_query(sql)
        count = len(df)
        msg   = f"{count} things found" if count > 0 else "Nothing found"
        return _result("ALERT" if count > 0 else "CLEAR", msg, df, count)
    except Exception as exc:
        return _error(exc)
```

2. **`queries.py`** — import the function at the top of `app.py` and add it to `SIGNAL_FNS`:
```python
"my_signal": check_my_signal,
```

3. **`app.py`** — add a `signal_card()` call in the appropriate section:
```python
signal_card(
    icon="🔍",
    title="My Signal",
    description="What this checks and when it alerts.",
    result=results["my_signal"],
)
```

4. **`app.py`** — add an entry to the `signals_info` list on the landing page.

---

## Known Constraints & Workarounds

| Constraint | Workaround |
|------------|------------|
| `prod_raw.homebase1.*` — access restricted | Use `prod_redshift_replica.postgres.locations` instead |
| `prod_enriched.amplitude.*` — access restricted | Use `prod_redshift_replica.dbt_staging.s_amp_owner_signups_raw` |
| `jobs.level` is title-cased (`'Manager'`, `'Employee'`) | All level filters use `LOWER(j.level)` for safe comparison |
| `SELECT DISTINCT` + `ORDER BY` in Databricks SQL | Must `ORDER BY` the column alias, not the original `table.column` reference |
| `hiring_job_requests.created_at` is already a TIMESTAMP | Do **not** wrap in `TIMESTAMP_MICROS()` — it will error |
| Stripe customer not found via metadata alone | Signal 15 uses a three-way UNION: subscription metadata + charge metadata + `stripe.customer.email` matched against manager account email |
