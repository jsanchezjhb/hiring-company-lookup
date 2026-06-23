"""
app.py — Hiring Fraud Detection Tool
Homebase Service Team Internal App

Entry point for the Databricks App. Run with:
  streamlit run app.py
"""

import streamlit as st
import pandas as pd

from queries import (
    get_company_info,
    check_active_job_posts,
    check_rapid_postings,
    check_hourly_burst,
    check_ip_location_mismatch,
    check_dormancy_reactivation,
    check_failed_billing,
    check_billing_disputes,
    check_payment_method_changes,
    check_fingerprint_reuse,
    check_suspicious_email_domains,
    check_owner_verification,
    check_employee_verification,
    check_suspicious_timecards,
    check_employee_documents,
    check_payment_method_on_file,
)

# ─── Page config (must be first Streamlit call) ───────────────────────────────
st.set_page_config(
    page_title="Fraud Detection | Hiring",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─── Stylesheet ───────────────────────────────────────────────────────────────
st.markdown("""
<style>
/* ── Base ── */
[data-testid="stAppViewContainer"] { background-color: #F7F8FA; }
[data-testid="stHeader"] { display: none; }
footer { visibility: hidden; }
#MainMenu { visibility: hidden; }

/* ── App header ── */
.app-title {
    font-size: 26px;
    font-weight: 800;
    color: #0F172A;
    letter-spacing: -0.5px;
}
.app-subtitle {
    font-size: 14px;
    color: #64748B;
    margin-top: -8px;
    margin-bottom: 20px;
}

/* ── Company info bar ── */
.company-bar {
    background: #0F172A;
    color: #fff;
    padding: 14px 20px;
    border-radius: 10px;
    margin-bottom: 20px;
    display: flex;
    gap: 40px;
    align-items: center;
}
.company-bar-label { font-size: 11px; color: #94A3B8; text-transform: uppercase; letter-spacing: 0.5px; }
.company-bar-value { font-size: 16px; font-weight: 700; color: #fff; }

/* ── Risk banner ── */
.risk-high   { border-left: 6px solid #EF4444; background:#FEF2F2; padding:12px 18px; border-radius:0 8px 8px 0; }
.risk-medium { border-left: 6px solid #F59E0B; background:#FFFBEB; padding:12px 18px; border-radius:0 8px 8px 0; }
.risk-low    { border-left: 6px solid #22C55E; background:#F0FDF4; padding:12px 18px; border-radius:0 8px 8px 0; }

/* ── Signal card states ── */
.card-alert   { border-left: 5px solid #EF4444; background:#FFF5F5; padding:14px 18px; border-radius:0 8px 8px 0; margin-bottom:8px; }
.card-clear   { border-left: 5px solid #22C55E; background:#F0FDF4; padding:14px 18px; border-radius:0 8px 8px 0; margin-bottom:8px; }
.card-pending { border-left: 5px solid #94A3B8; background:#F8FAFC; padding:14px 18px; border-radius:0 8px 8px 0; margin-bottom:8px; }
.card-error   { border-left: 5px solid #F59E0B; background:#FFFBEB; padding:14px 18px; border-radius:0 8px 8px 0; margin-bottom:8px; }

/* ── Status pill ── */
.pill {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 999px;
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 0.5px;
    text-transform: uppercase;
}
.pill-alert   { background:#EF4444; color:#fff; }
.pill-clear   { background:#22C55E; color:#fff; }
.pill-pending { background:#94A3B8; color:#fff; }
.pill-error   { background:#F59E0B; color:#fff; }

/* ── Section divider ── */
.section-label {
    font-size: 13px;
    font-weight: 700;
    color: #475569;
    text-transform: uppercase;
    letter-spacing: 1px;
    margin: 24px 0 10px 0;
}
</style>
""", unsafe_allow_html=True)


# ─── Signal card renderer ─────────────────────────────────────────────────────

def signal_card(
    icon: str,
    title: str,
    description: str,
    result: dict | None = None,
    *,
    expand_on_alert: bool = True,
):
    """
    Render a collapsible fraud signal card.

    Args:
        icon:             Emoji icon string
        title:            Display name of the signal
        description:      One-sentence explanation of what is checked
        result:           Dict returned by a signal check function, or None
        expand_on_alert:  Auto-expand the card when status is ALERT
    """
    if result is None:
        # Should not happen in normal flow, treat as pending
        result = {"status": "PENDING", "message": "Not yet run.", "detail_df": pd.DataFrame(), "alert_count": 0}

    status      = result.get("status", "ERROR")
    message     = result.get("message", "")
    detail_df   = result.get("detail_df", pd.DataFrame())
    alert_count = result.get("alert_count", 0)

    # Emoji badge
    badge = {
        "ALERT":   "🚨",
        "CLEAR":   "✅",
        "PENDING": "⏳",
        "ERROR":   "⚠️",
    }.get(status, "❓")

    pill_class = {
        "ALERT":   "pill-alert",
        "CLEAR":   "pill-clear",
        "PENDING": "pill-pending",
        "ERROR":   "pill-error",
    }.get(status, "pill-pending")

    expander_title = f"{icon}  **{title}**  —  {badge} {status}  —  {message}"
    auto_expand    = False  # all cards collapsed by default; user clicks to expand

    with st.expander(expander_title, expanded=auto_expand):

        col_desc, col_badge = st.columns([3, 1])

        with col_desc:
            st.markdown(f"<span style='font-size:13px;color:#475569;'>{description}</span>",
                        unsafe_allow_html=True)
            st.markdown(f"**Result:** {message}")

        with col_badge:
            st.markdown(
                f"<span class='pill {pill_class}'>{status}</span>",
                unsafe_allow_html=True,
            )
            if status == "ALERT":
                st.markdown(
                    f"<span style='font-size:28px;font-weight:800;color:#EF4444;'>{alert_count}</span>"
                    f"<span style='font-size:12px;color:#94A3B8;'> item(s)</span>",
                    unsafe_allow_html=True,
                )

        # ── Drill-down table ──────────────────────────────────────────
        if status == "ALERT" and not detail_df.empty:
            st.markdown("---")
            st.markdown(f"**📋 Detailed Results** ({len(detail_df)} rows)")
            st.dataframe(
                detail_df,
                use_container_width=True,
                hide_index=True,
            )

        elif status == "PENDING":
            st.info(f"⏳ {message}")

        elif status == "ERROR":
            st.warning(f"⚠️ {message}")


# ─── Main app ─────────────────────────────────────────────────────────────────

def main():
    # ── App header ──────────────────────────────────────────────────────────
    st.markdown('<div class="app-title">🔍 Hiring Fraud Detection Tool</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="app-subtitle">Enter a Company ID to run all fraud and suspicious-activity checks</div>',
        unsafe_allow_html=True,
    )

    # ── Input row ───────────────────────────────────────────────────────────
    col_id, col_btn, col_refresh, col_spacer = st.columns([2, 1, 1, 5])

    with col_id:
        raw_id = st.text_input(
            "Company ID",
            placeholder="e.g. 123456",
            label_visibility="collapsed",
        )
    with col_btn:
        run_btn = st.button("🔍 Analyze", type="primary", use_container_width=True)
    with col_refresh:
        refresh_btn = st.button("🔄 Refresh", use_container_width=True, help="Clear cache and re-run")

    if refresh_btn:
        st.cache_data.clear()
        st.rerun()

    # ── Guard: no input yet ──────────────────────────────────────────────────
    if not run_btn and not st.session_state.get("last_company_id"):
        st.markdown("---")
        st.markdown("#### 👋 How to use this tool")
        st.markdown("""
1. Enter a **Company ID** in the field above and click **Analyze**
2. Results appear below — 🚨 **ALERT** cards auto-expand for quick triage
3. Click any card to expand it and view the full detail table
4. Click **Refresh** at any time to re-run the analysis with fresh data
        """)
        st.markdown("---")
        st.markdown(
            "<div class='section-label'>Signals covered</div>",
            unsafe_allow_html=True,
        )
        signals_info = [
            ("📋", "Active Job Posts",              "10+ active jobs at the same time"),
            ("⚡", "Rapid Posting",                 "Multiple jobs created < 1 minute apart"),
            ("📈", "Hourly Burst",                  "4+ jobs posted within a single hour"),
            ("🌐", "IP / Location Mismatch",        "Account IP doesn't match company city/state"),
            ("💤", "Dormancy Reactivation",          "30+ day gap then sudden job posting"),
            ("💳", "Failed Billing",                "Unsuccessful Stripe billing attempts"),
            ("⚖️", "Billing Disputes",              "Open or resolved billing disputes"),
            ("🔄", "Payment Method Changes",        "3+ payment method changes on Stripe"),
            ("🫂", "Stripe Fingerprint Reuse",      "Same card used across multiple company accounts"),
            ("📧", "Suspicious Email Domains",      "Owner or employees using known fraud-associated domains"),
            ("👤", "Owner Verification",            "Owner email or phone not verified"),
            ("👥", "Employee Verification",         "Employee accounts with unverified contact details"),
            ("🕐", "Suspicious Timecard Overrides", "Manager entered 3+ punches in the last pay period"),
            ("📁", "Employee Documents",            "Onboarding documents pre-uploaded on a new account"),
            ("💰", "Payment Method on File",        "No Stripe payment method linked to this company"),
        ]
        for icon, name, desc in signals_info:
            st.markdown(f"- **{icon} {name}** — {desc}")
        return

    # ── Validate input ───────────────────────────────────────────────────────
    if run_btn:
        if not raw_id.strip():
            st.warning("Please enter a Company ID.")
            return
        try:
            company_id = int(raw_id.strip())
        except ValueError:
            st.error("❌ Company ID must be a number.")
            return
        st.session_state["last_company_id"] = company_id
    else:
        company_id = st.session_state.get("last_company_id")
        if not company_id:
            return

    # ── Company lookup ───────────────────────────────────────────────────────
    with st.spinner("Looking up company…"):
        try:
            company_df = get_company_info(company_id)
        except Exception as exc:
            st.error(f"❌ Could not connect to the warehouse: {exc}")
            st.stop()

    if company_df.empty:
        st.error(f"❌ No company found with ID **{company_id}**. Please double-check the ID.")
        return

    co = company_df.iloc[0]

    # ── Company info bar ─────────────────────────────────────────────────────
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Company",      co.get("company_name",  "—"))
    c2.metric("Company ID",   company_id)
    c3.metric("Locations",    co.get("location_count", "—"))
    c4.metric("Employees",    co.get("employee_count", "—"))
    c5.metric("Member Since", str(co.get("member_since", "—"))[:10])

    st.divider()

    # ── Run all signal checks ────────────────────────────────────────────────
    st.markdown("### 🛡️ Fraud Signal Analysis")

    SIGNAL_FNS = {
        "active_jobs":          check_active_job_posts,
        "rapid_postings":       check_rapid_postings,
        "hourly_burst":         check_hourly_burst,
        "ip_mismatch":          check_ip_location_mismatch,
        "dormancy":             check_dormancy_reactivation,
        "failed_billing":       check_failed_billing,
        "disputes":             check_billing_disputes,
        "pm_changes":           check_payment_method_changes,
        "fingerprint_reuse":    check_fingerprint_reuse,
        "suspicious_domains":   check_suspicious_email_domains,
        "owner_verification":   check_owner_verification,
        "employee_verification": check_employee_verification,
        "suspicious_timecards": check_suspicious_timecards,
        "employee_documents":   check_employee_documents,
        "payment_method":       check_payment_method_on_file,
    }

    results: dict[str, dict] = {}

    with st.spinner("Running fraud signal checks…"):
        for key, fn in SIGNAL_FNS.items():
            try:
                results[key] = fn(company_id)
            except Exception as exc:
                results[key] = {
                    "status":      "ERROR",
                    "message":     str(exc),
                    "detail_df":   pd.DataFrame(),
                    "alert_count": 0,
                }

    # ── Risk score summary ───────────────────────────────────────────────────
    implemented_results = {k: v for k, v in results.items() if v["status"] != "PENDING"}
    alerts   = sum(1 for v in implemented_results.values() if v["status"] == "ALERT")
    analyzed = len(implemented_results)
    pending  = len(results) - analyzed

    if alerts >= 3:
        risk_level, risk_css, risk_emoji = "HIGH",   "risk-high",   "🔴"
    elif alerts >= 1:
        risk_level, risk_css, risk_emoji = "MEDIUM", "risk-medium", "🟡"
    else:
        risk_level, risk_css, risk_emoji = "LOW",    "risk-low",    "🟢"

    st.markdown(
        f"""
        <div class="{risk_css}" style="margin-bottom:20px;">
            <span style="font-size:22px;font-weight:800;">{risk_emoji} Risk Level: {risk_level}</span>
            &nbsp;&nbsp;
            <span style="color:#475569;font-size:14px;">
                {alerts} alert{'s' if alerts != 1 else ''} across {analyzed} active signal{'s' if analyzed != 1 else ''}
                &nbsp;•&nbsp;
                {pending} signal{'s' if pending != 1 else ''} pending implementation
            </span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Signal cards — Job Posting Behaviour
    # ─────────────────────────────────────────────────────────────────────────
    st.markdown(
        "<div class='section-label'>📌 Job Posting Behaviour</div>",
        unsafe_allow_html=True,
    )

    signal_card(
        icon="📋",
        title="Active Job Posts",
        description=(
            "Flags companies with 10 or more currently active job postings. "
            "Legitimate businesses rarely maintain this many simultaneous open roles."
        ),
        result=results["active_jobs"],
    )

    signal_card(
        icon="⚡",
        title="Rapid Posting — Under 1 Minute Apart",
        description=(
            "Flags any two consecutive job posts created less than 60 seconds apart. "
            "This pattern is consistent with automated or bulk fraudulent submission."
        ),
        result=results["rapid_postings"],
    )

    signal_card(
        icon="📈",
        title="Hourly Posting Burst — 4+ Jobs in One Hour",
        description=(
            "Flags when 4 or more jobs were posted within the same clock hour. "
            "Expands to show every job in the flagged window."
        ),
        result=results["hourly_burst"],
    )

    signal_card(
        icon="💤",
        title="Dormancy Reactivation — 30+ Day Gap",
        description=(
            "Flags when a company resumes posting after 30+ days of no activity. "
            "May indicate account takeover, a reactivated fraud ring, or bot behaviour."
        ),
        result=results["dormancy"],
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Signal cards — Account & Identity
    # ─────────────────────────────────────────────────────────────────────────
    st.markdown(
        "<div class='section-label'>🌐 Account & Identity</div>",
        unsafe_allow_html=True,
    )

    signal_card(
        icon="🌐",
        title="IP / Location Mismatch",
        description=(
            "Compares the IP address at account creation against the company's registered city and state. "
            "Returns all rows — City Match, State Match Only, and No Match — so you can see the full picture. "
            "ALERTs on any 'No Match' result. "
            "The mismatch_pct column shows the heuristic fraud likelihood "
            "(e.g. known CDN cities like Ashburn score lower than a foreign-country IP)."
        ),
        result=results["ip_mismatch"],
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Signal cards — Account & Employee Risk
    # ─────────────────────────────────────────────────────────────────────────
    st.markdown(
        "<div class='section-label'>👤 Account & Employee Risk</div>",
        unsafe_allow_html=True,
    )

    signal_card(
        icon="📧",
        title="Suspicious Email Domains",
        description=(
            "Checks whether the owner or any employee account is using an email domain "
            "historically associated with fraudulent or disposable registrations "
            "(e.g. mail.com, engineer.com, usa.com). "
            "Flags all matching accounts with their role and domain."
        ),
        result=results["suspicious_domains"],
    )

    signal_card(
        icon="👤",
        title="Owner Email / Phone Verification",
        description=(
            "Checks whether the owner account has verified their email address (confirmed_at) "
            "and phone number (needs_phone_confirmation). "
            "An unverified owner on a new account is a strong identity risk signal."
        ),
        result=results["owner_verification"],
    )

    signal_card(
        icon="👥",
        title="Employee Email / Phone Verification",
        description=(
            "Flags any employee or manager account linked to this company that has not verified "
            "their email or phone. A high proportion of unverified employees on a new account "
            "may indicate bulk fake account creation."
        ),
        result=results["employee_verification"],
    )

    signal_card(
        icon="🕐",
        title="Suspicious Manager Timecard Overrides",
        description=(
            "Flags when a manager entered more than 3 timecard punches in the last 14 days "
            "(proxy for one pay period). Employees are expected to clock themselves in — "
            "a manager override is an exception. More than 3 in a pay period may indicate "
            "fabricated or adjusted records."
        ),
        result=results["suspicious_timecards"],
    )

    signal_card(
        icon="📁",
        title="Employee Onboarding Documents",
        description=(
            "Flags when onboarding documents are already uploaded for a new company. "
            "Pre-uploaded documents on a brand-new account may be forged or "
            "uploaded to falsely establish legitimacy — worth verifying."
        ),
        result=results["employee_documents"],
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Signal cards — Billing & Payments
    # ─────────────────────────────────────────────────────────────────────────
    st.markdown(
        "<div class='section-label'>💳 Billing & Payments</div>",
        unsafe_allow_html=True,
    )

    signal_card(
        icon="💰",
        title="Payment Method on File",
        description=(
            "Checks whether this company has a Stripe customer record with charges on file. "
            "A new company using a paid Hiring feature with no payment method configured "
            "is a potential fraud signal — they may not intend to pay."
        ),
        result=results["payment_method"],
    )

    signal_card(
        icon="💳",
        title="Failed Billing Attempts",
        description=(
            "Flags companies with one or more unsuccessful billing attempts from Stripe. "
            "Repeated failures may indicate stolen or invalid card details."
        ),
        result=results["failed_billing"],
    )

    signal_card(
        icon="⚖️",
        title="Billing Disputes",
        description=(
            "Flags companies with any open or resolved billing disputes. "
            "Chargebacks can indicate fraudulent account creation."
        ),
        result=results["disputes"],
    )

    signal_card(
        icon="🔄",
        title="Excessive Payment Method Changes",
        description=(
            "Flags companies that have changed their payment method more than 2 times. "
            "High turnover may indicate card-testing behaviour."
        ),
        result=results["pm_changes"],
    )

    signal_card(
        icon="🫂",
        title="Stripe Fingerprint Reuse Across Accounts",
        description=(
            "Flags when this company's Stripe card fingerprint also appears on one or more "
            "other company accounts. The same physical card being used across separate accounts "
            "is a strong indicator of a fraud ring or bulk account creation. "
            "Stripe fingerprints are stable even when a card is re-issued with a new expiry date."
        ),
        result=results["fingerprint_reuse"],
    )

    # ── Footer ───────────────────────────────────────────────────────────────
    st.divider()
    st.markdown(
        f"<span style='font-size:12px;color:#94A3B8;'>"
        f"Analysis complete · Company ID: {company_id} · "
        f"{analyzed} of {len(results)} signals active · "
        f"Powered by Homebase Databricks"
        f"</span>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
