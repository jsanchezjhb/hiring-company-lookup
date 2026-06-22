# Hiring Fraud Detection App

Internal Homebase tool for the service team to investigate suspicious company behaviour in the Hiring product. Enter a Company ID to run all fraud signal checks and see results with drill-down details.

---

## Signal Status

| # | Signal | Status | Table(s) |
|---|--------|--------|----------|
| 1 | 10+ Active Job Posts | ✅ Live | `prod_redshift_replica.postgres.hiring_job_requests` |
| 2 | Jobs Posted < 1 Min Apart | ✅ Live | `prod_redshift_replica.postgres.hiring_job_requests` |
| 3 | 4+ Jobs Within One Hour | ✅ Live | `prod_redshift_replica.postgres.hiring_job_requests` |
| 4 | IP / Location Mismatch | ✅ Live | `prod_redshift_replica.heap.sign_up_owner_signed_up`, `prod_redshift_replica.homebase1.locations` |
| 5 | Dormancy Reactivation (30+ days) | ✅ Live | `prod_redshift_replica.postgres.hiring_job_requests` |
| 6 | Failed Billing | ✅ Live | `prod_redshift_replica.stripe.charge`, `prod_redshift_replica.stripe.customer_subscription` |
| 7 | Billing Disputes | ✅ Live | `prod_redshift_replica.stripe.charge`, `prod_redshift_replica.stripe.customer_subscription` |
| 8 | Payment Method Changes (3+) | ✅ Live | `prod_redshift_replica.stripe.charge`, `prod_redshift_replica.stripe.customer_subscription` |
| 9 | Stripe Fingerprint Reuse | ✅ Live | `prod_redshift_replica.stripe.charge`, `prod_redshift_replica.stripe.customer_subscription` |

All signals use the `prod_redshift_replica` catalog.

---

## File Structure

```
hiring-company-lookup/
├── app.py            Main Streamlit UI — signal cards, risk banner, layout
├── queries.py        All 9 fraud signal SQL functions + DB connection
├── requirements.txt  Python dependencies
├── app.yaml          Tells Databricks to run with Streamlit (required)
└── README.md         This file
```

---

## Deployment

### Prerequisites
- Access to Databricks workspace `homebase-staging.cloud.databricks.com`
- The app is connected to the `readers-staging-sqlwh-01` SQL Warehouse

### Steps

1. Push all files to the GitHub repo (`hiring-company-lookup`, `main` branch)
2. In Databricks → **Apps** → `hiring-company-lookup` → **Edit**
3. Verify **Resources** tab has `readers-staging-sqlwh-01` with **Can Use**
4. Verify **Environment Variables** has:
   - `SQL_WAREHOUSE_HTTP_PATH` = `/sql/1.0/warehouses/16984dfe9a2c3705`
5. Verify **User Authorization** is **ON**
6. Click **Deploy**

### app.yaml
The `app.yaml` file must exist and contain only:
```yaml
command:
  - streamlit
  - run
  - app.py
```
Without it, Databricks runs `python app.py` instead of Streamlit and the app crashes.

---

## Authentication

The app uses **User Authorization** (Apps → Edit → User Authorization = ON).

Databricks injects the logged-in user's OAuth token via the `X-Forwarded-Access-Token` request header. The app passes this token to the SQL connector using `Config(token=user_token)` + `credentials_provider=lambda: cfg.authenticate` — the same pattern used by the billing-disputes app.

Queries run as the logged-in user and inherit their Unity Catalog permissions. No service principal grants needed.

---

## Key SQL Rules

**Hiring job queries (Signals 1–5)**
```sql
-- Always filter
WHERE hjr.hiring_version = 2
  AND hjr.status != 'draft'
  AND (hjr.activated_at IS NOT NULL OR hjr.flagged_at IS NOT NULL)
  AND c.company_id != 1987234        -- exclude test company

-- created_at is already a TIMESTAMP — no conversion needed
CAST(hjr.created_at AS DATE)         -- date
UNIX_TIMESTAMP(created_at)           -- seconds (for gap calculations)
DATEDIFF(created_at, prev_created_at) -- day gaps

-- Jobs scope: company → locations → job_requests
INNER JOIN prod_redshift_replica.public.locations l ON l.location_id = hjr.location_id
INNER JOIN prod_redshift_replica.public.companies c ON c.company_id = l.company_id
```

**Stripe queries (Signals 6–9)**
```sql
-- Company → Stripe customer mapping
-- company_id is stored as a JSON string inside the metadata column
SELECT DISTINCT customer
FROM prod_redshift_replica.stripe.customer_subscription
WHERE GET_JSON_OBJECT(metadata, '$.company_id') = '{company_id}'

-- charge.created is Unix epoch seconds
FROM_UNIXTIME(c.created)            -- convert to timestamp

-- Card fingerprint is nested JSON
GET_JSON_OBJECT(payment_method_details, '$.card.fingerprint')
```

---

## Adding a New Signal

1. Open `queries.py`
2. Add a new function following the existing pattern — return `_result("ALERT"/"CLEAR", message, df, count)`
3. Add the function to `SIGNAL_FNS` dict in `app.py`
4. Add a `signal_card(...)` call in the appropriate section of `app.py`
5. Deploy

---

## Databricks Workspace Info

| Setting | Value |
|---------|-------|
| Host | `homebase-staging.cloud.databricks.com` |
| Warehouse | `readers-staging-sqlwh-01` |
| HTTP Path | `/sql/1.0/warehouses/16984dfe9a2c3705` |
| Primary Catalog | `prod_redshift_replica` |
| App Service Principal | `195f838c-927a-45f9-94f4-2b59a9c7a453` |
