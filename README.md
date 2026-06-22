# Hiring Fraud Detection App

Internal Homebase tool for the service team to investigate suspicious company behaviour in the Hiring product.

---

## Signal Status

| # | Signal | Status | Table(s) Used |
|---|--------|--------|---------------|
| 1 | 10+ Active Job Posts | ✅ Implemented | `postgres.hiring_job_requests`, `public.locations`, `public.companies` |
| 2 | Jobs Posted < 1 Min Apart | ✅ Implemented | `postgres.hiring_job_requests`, `public.locations`, `public.companies` |
| 3 | 4+ Jobs Within One Hour | ✅ Implemented | `postgres.hiring_job_requests`, `public.locations`, `public.companies` |
| 4 | IP / Location Mismatch | ⏳ Pending | Needs existing query from user |
| 5 | Dormancy Reactivation (30+ days) | ✅ Implemented | `postgres.hiring_job_requests`, `public.locations`, `public.companies` |
| 6 | Failed Billing | ⏳ Pending | Needs Stripe table name(s) |
| 7 | Billing Disputes | ⏳ Pending | Needs Stripe table name(s) |
| 8 | 3+ Payment Method Changes | ⏳ Pending | Needs Stripe table name(s) |
| 9 | Stripe Fingerprint Reuse Across Accounts | ⏳ Pending | Needs Stripe payment methods table — query is written and ready |

---

## File Structure

```
fraud_detection_app/
├── app.py            Main Streamlit UI
├── queries.py        All fraud signal SQL functions
├── db_utils.py       Databricks SQL connection utility
├── requirements.txt  Python dependencies
├── app.yaml          Databricks App configuration
└── README.md         This file
```

---

## Deployment — Databricks Apps

### Step 1 — Upload files to your Databricks workspace

1. In the Databricks UI, go to **Workspace** (left sidebar)
2. Navigate to `/Users/your-email/` (or a shared folder)
3. Create a folder called `fraud-detection-app`
4. Upload all 5 files: `app.py`, `queries.py`, `db_utils.py`, `requirements.txt`, `app.yaml`

### Step 2 — Find your SQL Warehouse HTTP Path

1. In the Databricks UI, go to **SQL Warehouses**
2. Click your warehouse → **Connection Details** tab
3. Copy the **HTTP Path** (looks like `/sql/1.0/warehouses/abc123def456`)

### Step 3 — Update app.yaml

Replace `YOUR_WAREHOUSE_ID_HERE` in `app.yaml` with the HTTP path you copied.

### Step 4 — Create the Databricks App

1. In the Databricks UI, go to **Apps** (left sidebar)
2. Click **Create App**
3. Select **Custom** → point it to your workspace folder
4. Click **Deploy**

Databricks Apps handles all authentication automatically via OAuth — no token needed.

---

## Local Development

### Setup

```bash
# 1. Clone / copy the files to a local directory
cd fraud_detection_app

# 2. Install dependencies
pip install -r requirements.txt

# 3. Set environment variables
export DATABRICKS_HOST="homebase-staging.cloud.databricks.com"
export SQL_WAREHOUSE_HTTP_PATH="/sql/1.0/warehouses/your-warehouse-id"
export DATABRICKS_TOKEN="your-pat-token-here"

# 4. Run
streamlit run app.py
```

---

## Adding a New Signal

1. Open `queries.py`
2. Copy the stub pattern from any `_pending()` function
3. Write the SQL, following the Databricks SQL rules:
   - Always filter `hiring_version = 2`
   - Use `TIMESTAMP_MICROS(hjr.created_at)` for microsecond timestamps
   - Use `INNER JOIN` on `public.companies` and `public.locations` (excludes fake data)
   - Exclude test company: `AND c.company_id != 1987234`
4. Return a result dict via `_result("ALERT" or "CLEAR", message, df, count)`
5. In `app.py`, replace the `result=results["signal_key"]` for the card with the real function

---

## SQL Rules Quick Reference

```sql
-- Always include
WHERE hjr.hiring_version = 2
  AND hjr.status != 'draft'
  AND (hjr.activated_at IS NOT NULL OR hjr.flagged_at IS NOT NULL)
  AND c.company_id != 1987234  -- exclude test company

-- Microsecond timestamp conversion
TIMESTAMP_MICROS(hjr.created_at)        -- returns TIMESTAMP
CAST(TIMESTAMP_MICROS(hjr.created_at) AS DATE)  -- returns DATE

-- Compute second gap between two microsecond timestamps
(newer_micros - older_micros) / 1_000_000        -- seconds
(newer_micros - older_micros) / 60_000_000       -- minutes
(newer_micros - older_micros) / 86_400_000_000   -- days

-- Subscriptions are at LOCATION level, not company level
WHERE bps.subscription_type = 'hiring_assistant'
  AND bps.owner_type = 'Location'
  AND bps.archived_at IS NULL  -- active only
```
