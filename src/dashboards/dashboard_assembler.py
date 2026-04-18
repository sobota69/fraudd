"""Dashboard assembler – builds the FRAML Alert Dashboard in Streamlit.

Implements the HACKATHON_ALERT DASHBOARD requirements:
  1. Transaction Alert List (mandatory)
  2. Summary Metrics (mandatory): alerts by risk level, rule trigger counts
  3. Filtering & Sorting (optional)
  4. Visualizations (optional): alert trends, risk distribution, rule frequency
"""

from __future__ import annotations

from io import BytesIO
from typing import List

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from src.risk.risk_calculator import RiskAssessment
from src.rules.base_rule import RuleResult
from src.workflow_runner import WorkflowResult


# ── Colour palette for risk levels ───────────────────────────────────────────
_RISK_COLOURS = {"HIGH": "#EF553B", "MEDIUM": "#FFA15A", "LOW": "#636EFA"}
_SEVERITY_ICON = {"HIGH": "🔴", "MEDIUM": "🟠", "LOW": "🔵"}


def _build_alerts_df(result: WorkflowResult) -> pd.DataFrame:
    """Build a flat alerts DataFrame from pre-computed WorkflowResult."""
    alert_rows: list[dict] = []
    for tx, assessment, rule_results in zip(
        result.transactions, result.assessments, result.rule_results
    ):
        for rr in rule_results:
            if rr.triggered:
                alert_rows.append(
                    {
                        "transaction_id": tx.transaction_id,
                        "timestamp": tx.transaction_timestamp,
                        "customer_id": tx.customer_id,
                        "amount": tx.amount,
                        "currency": tx.currency,
                        "channel": tx.channel,
                        "beneficiary_account": tx.beneficiary_account,
                        "risk_score": assessment.risk_score,
                        "risk_level": assessment.risk_category,
                        "rule_id": rr.rule_id,
                        "rule_name": rr.rule_name,
                        "severity": rr.severity.name if rr.severity else "",
                        "weight": rr.weight,
                        "is_fraud": assessment.is_fraud_transaction,
                        "explanation": _build_explanation(rr),
                    }
                )
    return pd.DataFrame(alert_rows) if alert_rows else pd.DataFrame()


def _build_explanation(rr: RuleResult) -> str:
    """Human-readable explanation of why a rule fired."""
    parts = [f"{rr.rule_name}"]
    if rr.details:
        for k, v in rr.details.items():
            parts.append(f"{k}: {v}")
    return " | ".join(parts)


# ── Public entry point ───────────────────────────────────────────────────────
def render_alert_dashboard(result: WorkflowResult) -> None:
    """Render the full FRAML Alert Dashboard from pre-computed WorkflowResult."""

    st.header("🛡️ FRAML Alert Dashboard")
    st.success(
        f"✅ {len(result.transactions):,} transactions processed in {result.elapsed:.2f}s."
    )

    alerts_df = _build_alerts_df(result)

    if alerts_df.empty:
        st.success("✅ No rules triggered – all transactions passed.")
        return

    # Build per-transaction risk summary (de-duplicated)
    risk_df = pd.DataFrame(
        [
            {
                "transaction_id": a.transaction_id,
                "risk_score": a.risk_score,
                "risk_level": a.risk_category,
                "is_fraud": a.is_fraud_transaction,
                "triggered_rules": a.triggered_rules,
            }
            for a in result.assessments
        ]
    )

    # ── 1. Summary Metrics (mandatory) ───────────────────────────────────────
    st.subheader("📊 Summary Metrics")

    # Alerts by risk level
    flagged = risk_df[risk_df["risk_score"] > 0]
    risk_counts = flagged["risk_level"].value_counts().reindex(
        ["HIGH", "MEDIUM", "LOW"], fill_value=0
    )

    if "is_fraud" in risk_df.columns:
        fraud_col = risk_df["is_fraud"].map({"True": True, "False": False, True: True, False: False}).fillna(False)
        fraud_count = int(fraud_col.sum())
    else:
        fraud_count = 0

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Total Alerts", f"{len(flagged):,}")
    m2.metric("🚨 Fraud", f"{fraud_count:,}")
    m3.metric(f"{_SEVERITY_ICON['HIGH']} High", f"{risk_counts.get('HIGH', 0):,}")
    m4.metric(f"{_SEVERITY_ICON['MEDIUM']} Medium", f"{risk_counts.get('MEDIUM', 0):,}")
    m5.metric(f"{_SEVERITY_ICON['LOW']} Low", f"{risk_counts.get('LOW', 0):,}")

    # Number of triggered rules
    st.markdown("**Triggers per rule:**")
    rule_trigger_counts = (
        alerts_df.groupby(["rule_id", "rule_name"])
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )
    cols = st.columns(min(len(rule_trigger_counts), 5))
    for i, (_, row) in enumerate(rule_trigger_counts.head(5).iterrows()):
        cols[i].metric(row["rule_id"], f"{row['count']:,}", help=row["rule_name"])

    if len(rule_trigger_counts) > 5:
        with st.expander("All rule trigger counts"):
            st.dataframe(rule_trigger_counts, use_container_width=True)

    st.markdown("---")

    # ── 2. Filtering & Sorting ───────────────────────────────────────────────
    st.subheader("🔍 Filters")
    f1, f2, f3, f4, f5 = st.columns(5)

    with f1:
        risk_filter = st.multiselect(
            "Risk level",
            options=["HIGH", "MEDIUM", "LOW"],
            default=["HIGH", "MEDIUM", "LOW"],
        )
    with f2:
        rule_filter = st.multiselect(
            "Rule",
            options=sorted(alerts_df["rule_id"].unique()),
            default=sorted(alerts_df["rule_id"].unique()),
        )
    with f3:
        channel_opts = sorted(alerts_df["channel"].unique())
        channel_filter = st.multiselect("Channel", options=channel_opts, default=channel_opts)
    with f4:
        fraud_filter = st.selectbox("Fraud", options=["All", "Fraud only", "Non-fraud only"], index=0)
    with f5:
        min_amount, max_amount = float(alerts_df["amount"].min()), float(
            alerts_df["amount"].max()
        )
        amount_range = st.slider(
            "Amount range",
            min_value=min_amount,
            max_value=max_amount,
            value=(min_amount, max_amount),
        )

    sort_col = st.selectbox(
        "Sort by",
        options=["risk_score", "amount", "timestamp"],
        index=0,
    )
    sort_asc = st.checkbox("Ascending", value=False)

    # Normalise is_fraud to bool
    if "is_fraud" in alerts_df.columns:
        alerts_df["is_fraud"] = alerts_df["is_fraud"].map({"True": True, "False": False, True: True, False: False}).fillna(False)
    else:
        alerts_df["is_fraud"] = False

    # Apply filters
    mask = (
        alerts_df["risk_level"].isin(risk_filter)
        & alerts_df["rule_id"].isin(rule_filter)
        & alerts_df["channel"].isin(channel_filter)
        & alerts_df["amount"].between(*amount_range)
    )
    if fraud_filter == "Fraud only":
        mask = mask & alerts_df["is_fraud"]
    elif fraud_filter == "Non-fraud only":
        mask = mask & ~alerts_df["is_fraud"]
    filtered_df = alerts_df[mask].sort_values(sort_col, ascending=sort_asc)

    st.markdown("---")

    # ── 3. Transaction Alert List (mandatory) ─────────────────────────────────
    st.subheader(f"🚨 Transaction Alert List ({len(filtered_df):,} of {len(alerts_df):,} alerts)")

    # Export to Excel
    export_buffer = BytesIO()
    export_df = filtered_df.copy()
    for col in export_df.select_dtypes(include=["datetimetz"]).columns:
        export_df[col] = export_df[col].dt.tz_localize(None)
    export_df.to_excel(export_buffer, index=False, sheet_name="Alerts")
    export_buffer.seek(0)
    st.download_button(
        label="📥 Export to Excel",
        data=export_buffer,
        file_name="filtered_alerts.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    # Show active filters summary
    active_filters = []
    if set(risk_filter) != {"HIGH", "MEDIUM", "LOW"}:
        active_filters.append(f"**Risk:** {', '.join(risk_filter)}")
    if len(rule_filter) < len(alerts_df["rule_id"].unique()):
        active_filters.append(f"**Rules:** {', '.join(rule_filter)}")
    if len(channel_filter) < len(alerts_df["channel"].unique()):
        active_filters.append(f"**Channel:** {', '.join(channel_filter)}")
    if fraud_filter != "All":
        active_filters.append(f"**Fraud:** {fraud_filter}")
    if amount_range != (min_amount, max_amount):
        active_filters.append(f"**Amount:** {amount_range[0]:,.0f} – {amount_range[1]:,.0f}")
    if sort_col != "risk_score" or sort_asc:
        active_filters.append(f"**Sort:** {sort_col} ({'↑' if sort_asc else '↓'})")

    if active_filters:
        st.caption("Active filters: " + "  •  ".join(active_filters))
    else:
        st.caption("No filters applied — showing all alerts")

    # Styled dataframe with colour-coded risk level
    display_cols = [
        "transaction_id",
        "timestamp",
        "customer_id",
        "amount",
        "currency",
        "channel",
        "risk_score",
        "risk_level",
        "is_fraud",
        "rule_id",
        "rule_name",
        "explanation",
    ]
    existing_cols = [c for c in display_cols if c in filtered_df.columns]

    def _highlight_risk(val):
        colour = _RISK_COLOURS.get(val, "")
        return f"background-color: {colour}; color: white; font-weight: bold" if colour else ""

    styled = (
        filtered_df[existing_cols]
        .style.map(_highlight_risk, subset=["risk_level"])
    )
    st.dataframe(styled, use_container_width=True, height=500)

    st.markdown("---")

    # ── 4. Visualizations (optional) ─────────────────────────────────────────
    st.subheader("📈 Visualizations")

    v1, v2 = st.columns(2)

    # 4a – Alerts by risk level (pie)
    with v1:
        risk_pie = filtered_df["risk_level"].value_counts().reset_index()
        risk_pie.columns = ["risk_level", "count"]
        fig = px.pie(
            risk_pie,
            names="risk_level",
            values="count",
            title="Alerts by Risk Level",
            hole=0.4,
            color="risk_level",
            color_discrete_map=_RISK_COLOURS,
        )
        st.plotly_chart(fig, use_container_width=True)

    # 4b – Rule trigger frequency (bar)
    with v2:
        rule_freq = (
            filtered_df.groupby(["rule_id", "rule_name"])
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=True)
        )
        fig = px.bar(
            rule_freq,
            x="count",
            y="rule_id",
            orientation="h",
            color="rule_name",
            title="Rule Trigger Frequency",
        )
        fig.update_layout(yaxis_title="Rule", xaxis_title="Triggers", showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    v3, v4 = st.columns(2)

    # 4c – Risk score distribution (histogram)
    with v3:
        fig = px.histogram(
            filtered_df.drop_duplicates("transaction_id"),
            x="risk_score",
            nbins=20,
            title="Risk Score Distribution",
            color_discrete_sequence=["#AB63FA"],
        )
        fig.update_layout(xaxis_title="Risk Score", yaxis_title="Transactions")
        st.plotly_chart(fig, use_container_width=True)

    # 4d – Alert trend over time (line)
    with v4:
        if "timestamp" in filtered_df.columns:
            trend = filtered_df.copy()
            trend["date"] = pd.to_datetime(trend["timestamp"]).dt.date
            daily_alerts = trend.groupby("date").size().reset_index(name="alerts")
            fig = px.line(
                daily_alerts,
                x="date",
                y="alerts",
                title="Alert Trend Over Time",
                markers=True,
                color_discrete_sequence=["#EF553B"],
            )
            fig.update_layout(xaxis_title="Date", yaxis_title="# Alerts")
            st.plotly_chart(fig, use_container_width=True)

    # ── 5. Fraud Analysis ────────────────────────────────────────────────────
    if "is_fraud" in filtered_df.columns:
        fraud_txs = filtered_df[filtered_df["is_fraud"] == True].drop_duplicates("transaction_id")
        if not fraud_txs.empty:
            st.markdown("---")
            st.subheader("🚨 Fraud Analysis")

            v1, v2 = st.columns(2)

            # Fraud transactions per customer
            with v1:
                fraud_per_cust = (
                    fraud_txs.groupby("customer_id")
                    .size()
                    .reset_index(name="fraud_count")
                    .sort_values("fraud_count", ascending=False)
                )
                fraud_per_cust["customer_id"] = fraud_per_cust["customer_id"].astype(str)
                fig = px.bar(
                    fraud_per_cust,
                    x="customer_id",
                    y="fraud_count",
                    title="Fraud Transactions per Customer (Top 20)",
                    color="fraud_count",
                    color_continuous_scale="Reds",
                )
                fig.update_layout(xaxis_title="Customer ID", yaxis_title="# Fraud Transactions")
                st.plotly_chart(fig, use_container_width=True)

            # Fraud transactions over time
            with v2:
                if "timestamp" in fraud_txs.columns:
                    fraud_trend = fraud_txs.copy()
                    fraud_trend["date"] = pd.to_datetime(fraud_trend["timestamp"]).dt.date
                    daily_fraud = fraud_trend.groupby("date").size().reset_index(name="fraud_count")
                    fig = px.line(
                        daily_fraud,
                        x="date",
                        y="fraud_count",
                        title="Fraud Transactions Over Time",
                        markers=True,
                        color_discrete_sequence=["#EF553B"],
                    )
                    fig.update_layout(xaxis_title="Date", yaxis_title="# Fraud Transactions")
                    st.plotly_chart(fig, use_container_width=True)

            # Fraud clients table
            st.markdown("**Clients with Fraud Detected**")
            # Count total transactions per customer from full dataset
            all_tx_counts = pd.Series(
                [tx.customer_id for tx in result.transactions]
            ).value_counts().rename("total_tx_count")

            deduped = filtered_df.drop_duplicates("transaction_id")
            per_client = (
                deduped.groupby("customer_id")
                .agg(
                    alert_count=("transaction_id", "count"),
                    fraud_transactions=("is_fraud", lambda s: int(s.sum())),
                )
                .reset_index()
            )
            per_client = per_client[per_client["fraud_transactions"] > 0].copy()
            per_client["total_transactions"] = per_client["customer_id"].map(all_tx_counts).fillna(0).astype(int)
            per_client["non_fraud"] = per_client["alert_count"] - per_client["fraud_transactions"]
            per_client["fraud_%"] = (per_client["fraud_transactions"] / per_client["total_transactions"] * 100).round(1)
            per_client = per_client.sort_values("fraud_transactions", ascending=False).reset_index(drop=True)
            per_client.index = per_client.index + 1
            per_client = per_client[["customer_id", "total_transactions", "alert_count", "fraud_transactions", "non_fraud", "fraud_%"]]
            per_client = per_client.rename(columns={
                "customer_id": "Customer ID",
                "total_transactions": "Total Transactions",
                "alert_count": "Total Alerts",
                "fraud_transactions": "Fraud",
                "non_fraud": "Non-Fraud",
                "fraud_%": "Fraud %",
            })
            styled_fraud = per_client.style.bar(subset=["Fraud %"], color="#EF553B80").format({"Fraud %": "{:.1f}%"})
            st.dataframe(styled_fraud, use_container_width=True)
