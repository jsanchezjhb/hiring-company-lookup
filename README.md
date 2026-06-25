# Hiring Fraud Detection Tool

Internal Streamlit app for the CS/Service team to investigate companies suspected of misusing Homebase's Hiring product. Enter a Company ID to run 21 fraud signals across identity, billing, job posting behaviour, and account activity.

**Live app:** `https://hiring-company-lookup-373323366197249.aws.databricksapps.com`
**Warehouse:** `readers-staging-sqlwh-01`
**Catalog:** `prod_redshift_replica`

---

## How to Use

1. Open the app and enter a **Company ID** on the home screen
2. Click **Run Analysis**
3. Signal cards stream in as each check completes — no waiting for all 21 to finish
4. Each card shows **ALERT** 🔴, **CLEAR** ✅, or **ERROR** ⚠️
5. Expand any card to see the full detail table
6. Use the **Export CSV** button on any table to pull the raw data

---

## Signal Reference

Signals are grouped into four sections, displayed in this order:

---

### 🌐 Account & Identity

| # | Signal | ALERT when |
|---|--------|------------|
| 1 | **IP / Location Mismatch** | Signup IP doesn't match registered city/state. Pulls from Heap (pre-2023) and Amplitude via `dbt_staging.s_amp_owner_signups_raw` (post-2023). Deduplicates on `(company_id, ip)` keeping the row with the most complete geo data. Includes `mismatch_pct` heuristic score. If no geo data found, falls back to `companies.onboard_source`. |
| 2 | **Device ID Reuse Across Companies** | The device used at signup (`heap_device_id` from Heap, `heap_id` from Amplitude) also appears in signup records for other companies. Indicates the same physical device was used to create multiple accounts. |
| 3 | **Owner Account Info Changes (Last 45 Days)** | Any sensitive field on the owner account was changed in the last 45 days: SSN, password, email, phone, or name. Sourced from `user_versions` audit log, filtered to `event = 'update'` only (excludes account creation). |
| 4 | **Account Change from Different IP** | A sensitive account change in the last 45 days was made from a different IP than the owner's original signup IP. Cross-references `user_versions.ip` against Heap and Amplitude signup IPs. |

---

### 👤 Account & Employee Risk

| # | Signal | ALERT when |
|---|--------|------------|
| 5 | **Suspicious Email Domains** | Owner or any employee using a known fraud-associated domain: `mail.com`, `engineer.com`, `usa.com`, `consultant.com`, `myself.com`, `dr.com`, `post.com`, `techie.com`, `writeme.com`, `cheerful.com` |
| 6 | **Manager Email / Phone Verification** | Any manager account has unverified email (`confirmed_at IS NULL`) or unverified phone (`needs_phone_confirmation = true`). Uses `LOWER(j.level) = 'manager'` and `j.archived_at IS NULL` to filter active managers only. |
| 7 | **Employee Email / Phone Verification** | Zero non-manager employees with contact details have verified email or phone. Shows verified vs. total count. Excludes shell accounts with no email AND no phone. |
| 8 | **Fully Manager-Created Timecards** | More than 3 employee timecards in the last 14 days where a manager created **both** the clock-in AND clock-out (`manager_added = true` in both `bizops.timecard_clockins` and `bizops.timecard_clockouts`). Partial edits and employee self-punches are excluded. |
| 9 | **Employee Onboarding Documents** | CLEAR (positive) if documents are on file. CLEAR (neutral) if none found — not inherently bad but provides no positive signal. |
| 10 | **Manager Linked to Other Companies** | Manager account(s) also appear at other companies via same account ID, matching email, or matching phone. Shows role at other company and match type. |
| 11 | **Employees at Multiple Other Companies** | Any of the first 10 employees (with email/phone) appears at more than 2 other companies. Matched by email or phone. `other_company_count` column shows severity. |

---

### 💳 Billing & Payments

| # | Signal | ALERT when |
|---|--------|------------|
| 12 | **Payment Method on File** | No Stripe customer found, OR customer exists but no payment method stored. Checks `customer_source` (legacy cards) and `payment_method` (all types including bank accounts). Customer lookup uses subscription + charge metadata only — email lookup removed after multi-customer false positive. |
| 13 | **Failed Billing Attempts** | One or more failed Stripe charges in the **last 6 months** |
| 14 | **Billing Disputes** | Any Stripe dispute in `stripe.i_charge_dispute` (all statuses). Joins directly on `customer_id` — does not rely on `charge.disputed` flag which is unreliable. |
| 15 | **Excessive Payment Method Changes** | More than 2 distinct payment methods used in the **last 6 months** |
| 16 | **Stripe Fingerprint Reuse** | Card fingerprint appears on another company's Stripe customer. Checks stored cards (`customer_source.fingerprint`), PaymentMethod API (`payment_method.card` JSON), and historical charges. One row per fingerprint+customer, showing the most direct match source. |

---

### 📌 Job Posting Behaviour

| # | Signal | ALERT when |
|---|--------|------------|
| 17 | **Active Job Posts** | 20 or more currently active job postings |
| 18 | **Rapid Posting — Under 1 Minute Apart** | Any two consecutive jobs created less than 60 seconds apart |
| 19 | **Hourly Posting Burst — 4+ Jobs in One Hour** | 4 or more jobs posted within the same clock hour |
| 20 | **Daily Posting Burst — 10+ Jobs in One Day** | 10 or more jobs posted on a single calendar day |
| 21 | **Dormancy Reactivation — 30+ Day Gap** | Any account had a 30+ day gap between consecutive sign-ins (`last_sign_in_at` → `current_sign_in_at`) AND posted Hiring jobs after coming back. Only considers returns within the **last 6 months**. |

---

## Key Tables

### Core Account & Job Data
| Table | Purpose |
|-------|---------|
| `prod_redshift_replica.postgres.accounts` | User accounts — `email`, `phone`, `confirmed_at`, `needs_phone_confirmation`, `first_name`, `last_name` |
| `prod_redshift_replica.postgres.jobs` | Employee↔Location link — `user_id`, `location_id`, `level` (title-cased: `'Manager'`, `'Employee'`), `archived_at` |
| `prod_redshift_replica.postgres.locations` | Location→Company link — `id`, `company_id`, `city`, `state`, `zip` |
| `prod_redshift_replica.postgres.companies` | Company record — `id`, `name`, `owner_id`, `onboard_source` |
| `prod_redshift_replica.public.companies` | Company name lookup — `company_id`, `name` |
| `prod_redshift_replica.postgres.hiring_job_requests` | Job postings — `created_at` is native TIMESTAMP, filter with `hiring_version = 2` |
| `prod_redshift_replica.postgres.account_sign_in_details` | Sign-in history — `current_sign_in_at`, `last_sign_in_at`, `sign_in_count` |
| `prod_redshift_replica.postgres.employee_onboarding_documents` | Document uploads — `user_id`, `company_id`, `category`, `filename` |
| `prod_redshift_replica.postgres.user_versions` | Audit log for account changes — `item_id`, `event`, `object_changes`, `ip`, `whodunnit`, `created_at` |

### Timecard Data
| Table | Purpose |
|-------|---------|
| `prod_redshift_replica.postgres.timecards` | Timecard records — `job_id`, `start_at`, `end_at`, `clock_in_source` |
| `prod_redshift_replica.bizops.timecard_clockins` | Clock-in audit flags — `timecard_id`, `manager_added`, `manager_edited`, `employee_added` |
| `prod_redshift_replica.bizops.timecard_clockouts` | Clock-out audit flags — `timecard_id`, `manager_added`, `manager_edited` |

### Signup Geo & Device
| Table | Purpose |
|-------|---------|
| `prod_redshift_replica.heap.sign_up_owner_signed_up` | Pre-2023 signups — `ip`, `heap_device_id`, `company_id` (string), `time` |
| `prod_redshift_replica.dbt_staging.s_amp_owner_signups_raw` | Post-2023 signups — `ip_address`, `heap_id`, `company_id` (bigint), `event_time` |

### Stripe
| Table | Purpose |
|-------|---------|
| `prod_redshift_replica.stripe.customer_subscription` | Company→customer link via `GET_JSON_OBJECT(metadata, '$.company_id')` |
| `prod_redshift_replica.stripe.charge` | Charge history — `status`, `created` (Unix epoch), `payment_method`, `payment_method_details` |
| `prod_redshift_replica.stripe.customer` | Stripe customer — `email`, `default_source` |
| `prod_redshift_replica.stripe.customer_source` | Legacy stored cards — `fingerprint`, `customer`, `row_deleted_at` |
| `prod_redshift_replica.stripe.payment_method` | PaymentMethod API — `card` (JSON with fingerprint), `us_bank_account`, `type`, `customer`, `row_deleted_at` |
| `prod_redshift_replica.stripe.i_charge_dispute` | Disputes — `dispute_id`, `charge_id`, `customer_id`, `amount`, `status`, `reason`, `created_at` |

---

## Architecture

```
app.py          — Streamlit UI: SECTIONS layout, streaming card rendering, parallel execution
queries.py      — All SQL + result logic; one function per signal
app.yaml        — Required by Databricks Apps: command: [streamlit, run, app.py]
requirements.txt
```

### Signal Execution — Parallel + Streaming
Signals run in parallel via `ThreadPoolExecutor(max_workers=5)` with a 90-second global timeout:

```python
pool = ThreadPoolExecutor(max_workers=5)
try:
    futures_map = {pool.submit(_run, item): item[0] for item in SIGNAL_FNS.items()}
    for future in as_completed(futures_map, timeout=90):
        key, result = future.result()
        results[key] = result
        # update placeholder immediately
except FuturesTimeoutError:
    pass  # timed-out keys already pre-populated with error result
finally:
    pool.shutdown(wait=False)  # abandon hanging threads — don't block page render
```

Key design decisions:
- All 21 result keys are **pre-populated** before threads run so rendering never hits a `KeyError`
- A **warm-up `SELECT 1`** runs before the pool starts to wake the warehouse and prevent cold-start timeouts
- `shutdown(wait=False)` ensures the page renders immediately even if threads are still hung
- Cards stream in as each signal completes — users see results progressively

### Auth Pattern (critical)
OAuth M2M via the Databricks SDK. **User Authorization must be OFF** in app settings — enabling it causes a "two auth methods" conflict.

```python
cfg  = Config()   # picks up DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET
conn = dbsql.connect(
    server_hostname=DATABRICKS_HOST,
    http_path=DATABRICKS_HTTP_PATH,
    credentials_provider=lambda: cfg.authenticate,
)
```

SDK versions that work: `databricks-sdk==0.20.0` + `databricks-sql-connector==3.1.0`

### Global Data Quality
`run_query()` applies three transforms to every result:
1. **Deduplication** — `df.drop_duplicates()` so duplicate rows never inflate counts
2. **ID formatting** — columns named `id` or ending in `_id` cast to string (prevents comma separators)
3. **Warm-up** — `SELECT 1` runs once before parallel execution to wake the warehouse

### Performance Patterns
Several signals use a **two-step approach** to avoid full table scans on large tables:
- Fetch a small set of IDs first (e.g. job IDs for a company)
- Pass those IDs as a literal `IN (...)` list to the second query
- Used by: Suspicious Timecards, Employee Documents, and others

All account/job queries use `locations → jobs → accounts` join order (small → large) rather than `accounts → jobs → locations` to ensure the company filter runs first.

---

## Adding a New Signal

1. **`queries.py`** — add a function:
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

2. **`app.py`** — import it and add to `SIGNAL_FNS` dict

3. **`app.py`** — add an entry to the `SECTIONS` list with `key`, `icon`, `title`, `description`

4. **`app.py`** — add an entry to the `signals_info` list on the landing page

---

## Known Constraints & Workarounds

| Constraint | Workaround |
|------------|------------|
| `prod_raw.homebase1.*` — access restricted | Use `prod_redshift_replica.postgres.locations` |
| `prod_enriched.amplitude.*` — access restricted | Use `prod_redshift_replica.dbt_staging.s_amp_owner_signups_raw` |
| `jobs.level` is title-cased (`'Manager'`, `'Employee'`) | All level filters use `LOWER(j.level)` |
| `SELECT DISTINCT` + `ORDER BY` in Databricks SQL | Must `ORDER BY` column alias, not `table.column` |
| `hiring_job_requests.created_at` is already a TIMESTAMP | Do **not** wrap in `TIMESTAMP_MICROS()` |
| Stripe customer lookup via email returns wrong customer | Email fallback removed — use subscription + charge metadata only |
| `charge.disputed` boolean unreliable in Fivetran sync | Query `stripe.i_charge_dispute` directly, join on `customer_id` |
| `timecard.clock_in_source` doesn't capture edits | Use `bizops.timecard_clockins.manager_added` / `manager_edited` instead |
| `postgres.companies` has `owner_id`; `public.companies` does not | Use `_PG_CO_TABLE` for owner lookups, `_CO_TABLE` for name lookups |
| Heap `company_id` is a string; Amplitude `company_id` is bigint | Use `TRY_CAST` when converting Heap's string company_id to bigint |
| Large tables (timecards, accounts) cause timeouts if company filter runs late | Always join `locations → jobs → accounts` (not `accounts → jobs → locations`) |
| ThreadPoolExecutor `with` block blocks on `__exit__` if threads hang | Use manual `pool.shutdown(wait=False)` instead of context manager |
