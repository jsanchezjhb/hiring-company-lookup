# Hiring Fraud Detection App

Internal Homebase tool for the service team to investigate suspicious company behaviour in the Hiring product. Enter a Company ID to run all fraud signal checks and see results at a glance — click any card to expand the full detail table.

---

## Signal Status

| # | Signal | Table(s) |
|---|--------|----------|
| 1 | 10+ Active Job Posts | `prod_redshift_replica.postgres.hiring_job_requests` |
| 2 | Jobs Posted < 1 Min Apart | `prod_redshift_replica.postgres.hiring_job_requests` |
| 3 | 4+ Jobs Within One Hour | `prod_redshift_replica.postgres.hiring_job_requests` |
| 4 | IP / Location Mismatch | `prod_redshift_replica.heap.sign_up_owner_signed_up`, `prod_redshift_replica.postgres.locations` |
| 5 | Dormancy Reactivation (30+ days) | `prod_redshift_replica.postgres.hiring_job_requests` |
| 6 | Failed Billing | `prod_redshift_replica.stripe.charge`, `prod_redshift_replica.stripe.customer_subscription` |
| 7 | Billing Disputes | `prod_redshift_replica.stripe.charge`, `prod_redshift_replica.stripe.customer_subscription` |
| 8 | Payment Method Changes (3+) | `prod_redshift_replica.stripe.charge`, `prod_redshift_replica.stripe.customer_subscription` |
| 9 | Stripe Fingerprint Reuse | `prod_redshift_replica.stripe.charge`, `prod_redshift_replica.stripe.customer_subscription` |

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

### Source
The app deploys from GitHub: `https://github.com/jsanchezjhb/hiring-company-lookup` (branch: `main`)

Push file changes to `main`, then click **Deploy** in Databricks Apps.

### Databricks App Settings
- **User Authorization**: OFF
- **Resources**: `readers-staging-sqlwh-01` with Can Use
- **Environment Variables**: `SQL_WAREHOUSE_HTTP_PATH` = `/sql/1.0/warehouses/16984dfe9a2c3705`

### app.yaml
Must exist with exactly:
```yaml
command:
  - streamlit
  - run
  - app.py
```
Without it, Databricks runs `python app.py` instead of Streamlit and the app crashes.

---

## Authentication

The app uses the service principal (`195f838c-927a-45f9-94f4-2b59a9c7a453`) via OAuth M2M — the same pattern as the billing-disputes app:

```python
from databricks.sdk.core import Config
from databricks import sql

cfg  = Config()
conn = sql.connect(
    server_hostname=DATABRICKS_HOST,
    http_path=DATABRICKS_HTTP_PATH,
    credentials_provider=lambda: cfg.authenticate,
)
```

**User Authorization must be OFF.** When it's on, Databricks injects `DATABRICKS_TOKEN` alongside `DATABRICKS_CLIENT_ID`/`SECRET`, causing a "two auth methods" conflict that breaks the SDK.

---

## Adding a New Signal

1. Add a function to `queries.py` following the existing pattern — return `_result("ALERT"/"CLEAR", message, df, count)`
2. Add it to `SIGNAL_FNS` dict in `app.py`
3. Add a `signal_card(...)` call in the appropriate section of `app.py`
4. Push to `main` and deploy

---

## Key SQL Patterns

**Hiring job queries (Signals 1–5)**
```sql
-- Always filter
WHERE hjr.hiring_version = 2
  AND hjr.status != 'draft'
  AND (hjr.activated_at IS NOT NULL OR hjr.flagged_at IS NOT NULL)
  AND c.company_id != 1987234        -- exclude test company

-- created_at is already a TIMESTAMP — no TIMESTAMP_MICROS() needed
UNIX_TIMESTAMP(created_at)            -- seconds (for gap calculations)
DATEDIFF(created_at, prev_created_at) -- day gaps

-- Always use full 3-part table names
INNER JOIN prod_redshift_replica.public.locations l  ON l.location_id = hjr.location_id
INNER JOIN prod_redshift_replica.public.companies c  ON c.company_id  = l.company_id
```

**Stripe queries (Signals 6–9)**
```sql
-- Company → Stripe customer mapping via metadata JSON
SELECT DISTINCT customer
FROM prod_redshift_replica.stripe.customer_subscription
WHERE GET_JSON_OBJECT(metadata, '$.company_id') = '{company_id}'

-- charge.created is Unix epoch seconds
FROM_UNIXTIME(c.created)

-- Card fingerprint is nested JSON
GET_JSON_OBJECT(payment_method_details, '$.card.fingerprint')
```

---

## Workspace Reference

| | |
|---|---|
| Host | `homebase-staging.cloud.databricks.com` |
| Warehouse | `readers-staging-sqlwh-01` |
| HTTP Path | `/sql/1.0/warehouses/16984dfe9a2c3705` |
| Catalog | `prod_redshift_replica` |
| App SP | `195f838c-927a-45f9-94f4-2b59a9c7a453` |
| GitHub | `github.com/jsanchezjhb/hiring-company-lookup` |
