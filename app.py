"""Streamlit dashboard for the Fraud Detection project."""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

from src.data_loader import generate_synthetic_data, preprocess, analyse_dataframe
from src.model import train_model
from rules import ALL_RULES
from transaction.transaction import Transaction
from datetime import datetime
from transaction.transaction import Transaction
from src.rules_runner import RulesRunner

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(page_title="Fraud Detection Dashboard", page_icon="🔍", layout="wide")
st.title("🔍 Fraud Detection Dashboard")

# ── Sidebar controls ────────────────────────────────────────────────────────
st.sidebar.header("⚙️ Settings")
uploaded_file = st.sidebar.file_uploader("Upload your own CSV (optional)", type=["csv"])

# Show synthetic-data controls only when no file is uploaded
if uploaded_file is None:
    n_samples = st.sidebar.slider("Number of transactions", 1_000, 50_000, 10_000, step=1_000)
    fraud_ratio = st.sidebar.slider("Fraud ratio", 0.01, 0.10, 0.02, step=0.01)
apply_smote = st.sidebar.checkbox("Apply SMOTE oversampling", value=True)

# ── Data loading ─────────────────────────────────────────────────────────────
@st.cache_data
def get_data(n: int, ratio: float, _file=None):
    if _file is not None:
        return pd.read_csv(_file)

    return generate_synthetic_data(n_samples=n, fraud_ratio=ratio)

if uploaded_file is not None:
    data_file = pd.read_csv(uploaded_file)
    transactions: list[Transaction] = []
    for record in data_file.to_dict(orient="records"):
        transactions.append(Transaction(**record))
    
    rules_runner = RulesRunner(rules=[RuleClass() for RuleClass in ALL_RULES])
    result = rules_runner.run_detection(transactions)

    df = get_data(0, 0, uploaded_file)
else:
    df = get_data(n_samples, fraud_ratio, None)

# Helper: check which expected synthetic columns exist
SYNTHETIC_COLS = {"is_fraud", "hour", "category", "distance_from_home", "amount",
                  "merchant_risk_score", "is_international", "num_transactions_last_hour"}
is_synthetic_schema = SYNTHETIC_COLS.issubset(set(df.columns))

# ── Uploaded file analysis ────────────────────────────────────────────────────
if uploaded_file is not None:
    st.header("🔎 Uploaded File Analysis")
    analysis = analyse_dataframe(df)

    # Key metrics
    a1, a2, a3, a4 = st.columns(4)
    a1.metric("Rows", f"{analysis['shape'][0]:,}")
    a2.metric("Columns", f"{analysis['shape'][1]:,}")
    a3.metric("Duplicate rows", f"{analysis['duplicates']:,}")
    total_cells = analysis["shape"][0] * analysis["shape"][1]
    miss_pct = (analysis["total_missing"] / total_cells * 100) if total_cells else 0
    a4.metric("Missing values", f"{analysis['total_missing']:,} ({miss_pct:.1f}%)")

    # Column types
    with st.expander("Column data types"):
        st.json(analysis["dtypes"])

    # Numeric statistics
    if not analysis["numeric_stats"].empty:
        with st.expander("Numeric column statistics", expanded=True):
            st.dataframe(analysis["numeric_stats"].style.format(precision=2), width='stretch')

    # Categorical statistics
    if not analysis["categorical_stats"].empty:
        with st.expander("Categorical column statistics", expanded=True):
            st.dataframe(analysis["categorical_stats"], width='stretch')

    # Correlation heatmap
    if not analysis["correlations"].empty:
        with st.expander("Correlation heatmap", expanded=True):
            fig = px.imshow(
                analysis["correlations"], text_auto=".2f",
                title="Feature Correlations", color_continuous_scale="RdBu_r",
                zmin=-1, zmax=1,
            )
            st.plotly_chart(fig, width='stretch')

    # Missing-value bar chart
    missing_series = df.isnull().sum()
    missing_series = missing_series[missing_series > 0]
    if not missing_series.empty:
        with st.expander("Missing values per column"):
            fig = px.bar(x=missing_series.index, y=missing_series.values,
                         labels={"x": "Column", "y": "Missing count"},
                         title="Missing Values by Column")
            st.plotly_chart(fig, width='stretch')

    # ── Smart visualisations for uploaded data ────────────────────────────────
    st.header("📈 Data Visualisations")

    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    categorical_cols = df.select_dtypes(include=["object", "category"]).columns.tolist()
    datetime_cols = []
    # Auto-detect datetime columns
    for c in df.columns:
        if df[c].dtype == "object":
            try:
                pd.to_datetime(df[c].dropna().head(20))
                datetime_cols.append(c)
            except Exception:
                pass

    # Parse timestamps if found
    ts_col = None
    if datetime_cols:
        ts_col = datetime_cols[0]
        df["_parsed_ts"] = pd.to_datetime(df[ts_col], errors="coerce")

    # -- Amount / numeric distribution -----------------------------------------
    if "amount" in df.columns:
        c1, c2 = st.columns(2)
        with c1:
            fig = px.histogram(df, x="amount", nbins=60, title="Transaction Amount Distribution",
                               color_discrete_sequence=["#636EFA"])
            fig.update_layout(xaxis_title="Amount", yaxis_title="Count")
            st.plotly_chart(fig, width='stretch')
        with c2:
            fig = px.box(df, y="amount", title="Amount Box Plot",
                         color_discrete_sequence=["#636EFA"])
            st.plotly_chart(fig, width='stretch')

    # -- Time-series charts ----------------------------------------------------
    if ts_col and "_parsed_ts" in df.columns:
        df["_hour"] = df["_parsed_ts"].dt.hour
        df["_date"] = df["_parsed_ts"].dt.date

        c1, c2 = st.columns(2)
        with c1:
            daily = df.groupby("_date").size().reset_index(name="count")
            fig = px.line(daily, x="_date", y="count", title="Transactions per Day",
                          markers=True, color_discrete_sequence=["#636EFA"])
            fig.update_layout(xaxis_title="Date", yaxis_title="Transaction Count")
            st.plotly_chart(fig, width='stretch')
        with c2:
            hourly = df.groupby("_hour").size().reset_index(name="count")
            fig = px.bar(hourly, x="_hour", y="count", title="Transactions by Hour of Day",
                         color_discrete_sequence=["#636EFA"])
            fig.update_layout(xaxis_title="Hour", yaxis_title="Count")
            st.plotly_chart(fig, width='stretch')

    # -- Channel distribution --------------------------------------------------
    if "channel" in df.columns:
        c1, c2 = st.columns(2)
        with c1:
            ch_counts = df["channel"].value_counts().reset_index()
            ch_counts.columns = ["channel", "count"]
            fig = px.pie(ch_counts, names="channel", values="count",
                         title="Transaction Channel Distribution", hole=0.4)
            st.plotly_chart(fig, width='stretch')
        with c2:
            if "amount" in df.columns:
                fig = px.box(df, x="channel", y="amount", color="channel",
                             title="Amount by Channel")
                fig.update_layout(showlegend=False)
                st.plotly_chart(fig, width='stretch')

    # -- Beneficiary name mismatch analysis ------------------------------------
    if "entered_beneficiary_name" in df.columns and "official_beneficiary_account_name" in df.columns:
        df["_name_mismatch"] = (
            df["entered_beneficiary_name"].str.strip().str.lower()
            != df["official_beneficiary_account_name"].str.strip().str.lower()
        )
        mismatch_count = df["_name_mismatch"].sum()
        st.subheader("🔀 Beneficiary Name Mismatch")
        mc1, mc2 = st.columns(2)
        mc1.metric("Mismatched names", f"{mismatch_count:,}")
        mc2.metric("Mismatch rate", f"{mismatch_count / len(df):.2%}")
        if mismatch_count > 0:
            with st.expander("Show mismatched rows"):
                st.dataframe(
                    df[df["_name_mismatch"]][
                        ["transaction_id", "entered_beneficiary_name",
                         "official_beneficiary_account_name", "amount"]
                    ].head(100) if "transaction_id" in df.columns else
                    df[df["_name_mismatch"]][
                        ["entered_beneficiary_name",
                         "official_beneficiary_account_name", "amount"]
                    ].head(100),
                    width='stretch',
                )

    # -- New beneficiary flag --------------------------------------------------
    if "is_new_beneficiary" in df.columns:
        c1, c2 = st.columns(2)
        with c1:
            new_ben = df["is_new_beneficiary"].value_counts().reset_index()
            new_ben.columns = ["is_new_beneficiary", "count"]
            fig = px.pie(new_ben, names="is_new_beneficiary", values="count",
                         title="New vs Existing Beneficiary", hole=0.4,
                         color_discrete_sequence=["#636EFA", "#EF553B"])
            st.plotly_chart(fig, width='stretch')
        with c2:
            if "amount" in df.columns:
                fig = px.box(df, x="is_new_beneficiary", y="amount",
                             color="is_new_beneficiary",
                             title="Amount by Beneficiary Status",
                             color_discrete_sequence=["#636EFA", "#EF553B"])
                fig.update_layout(showlegend=False)
                st.plotly_chart(fig, width='stretch')

    # -- Customer balance analysis ---------------------------------------------
    if "customer_account_balance" in df.columns:
        c1, c2 = st.columns(2)
        with c1:
            fig = px.histogram(df, x="customer_account_balance", nbins=50,
                               title="Account Balance Distribution",
                               color_discrete_sequence=["#00CC96"])
            st.plotly_chart(fig, width='stretch')
        with c2:
            if "amount" in df.columns:
                fig = px.scatter(df, x="customer_account_balance", y="amount",
                                 title="Balance vs Transaction Amount",
                                 opacity=0.4, color_discrete_sequence=["#636EFA"])
                st.plotly_chart(fig, width='stretch')

    # -- Top customers ---------------------------------------------------------
    if "customer_id" in df.columns:
        st.subheader("👤 Top Customers by Transaction Volume")
        top_cust = df["customer_id"].value_counts().head(15).reset_index()
        top_cust.columns = ["customer_id", "transactions"]
        top_cust["customer_id"] = top_cust["customer_id"].astype(str)
        fig = px.bar(top_cust, x="customer_id", y="transactions",
                     title="Top 15 Customers", color="transactions",
                     color_continuous_scale="Viridis")
        fig.update_layout(xaxis_title="Customer ID", yaxis_title="# Transactions")
        st.plotly_chart(fig, width='stretch')

    # -- Generic numeric distributions for remaining columns -------------------
    shown_numeric = {"amount", "customer_account_balance"}
    remaining_numeric = [c for c in numeric_cols if c not in shown_numeric and not c.startswith("_")]
    if remaining_numeric:
        with st.expander("Other numeric distributions"):
            for i in range(0, len(remaining_numeric), 2):
                cols = st.columns(2)
                for j, col in enumerate(cols):
                    idx = i + j
                    if idx < len(remaining_numeric):
                        with col:
                            fig = px.histogram(df, x=remaining_numeric[idx], nbins=40,
                                               title=f"{remaining_numeric[idx]} Distribution",
                                               color_discrete_sequence=["#AB63FA"])
                            st.plotly_chart(fig, width='stretch')

    # Clean up temp columns
    df.drop(columns=[c for c in df.columns if c.startswith("_")], inplace=True, errors="ignore")

    st.markdown("---")

# ── Data overview (always shown) ─────────────────────────────────────────────
st.header("📊 Data Overview")
if is_synthetic_schema:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total transactions", f"{len(df):,}")
    col2.metric("Fraudulent", f"{int(df['is_fraud'].sum()):,}")
    col3.metric("Legitimate", f"{int((df['is_fraud'] == 0).sum()):,}")
    col4.metric("Fraud %", f"{df['is_fraud'].mean():.2%}")
else:
    col1, col2 = st.columns(2)
    col1.metric("Total transactions", f"{len(df):,}")
    col2.metric("Columns", f"{len(df.columns):,}")

with st.expander("Show raw data sample"):
    st.dataframe(df.head(100))

# ── EDA charts (only for synthetic schema) ───────────────────────────────────
if is_synthetic_schema:
    st.header("📈 Exploratory Analysis")
    eda1, eda2 = st.columns(2)

    with eda1:
        fig = px.histogram(
            df, x="amount", color="is_fraud", nbins=60, barmode="overlay",
            title="Transaction Amount Distribution", labels={"is_fraud": "Fraud"},
            color_discrete_map={0: "#636EFA", 1: "#EF553B"},
        )
        fig.update_layout(xaxis_title="Amount ($)", yaxis_title="Count")
        st.plotly_chart(fig, width='stretch')

    with eda2:
        hourly = df.groupby(["hour", "is_fraud"]).size().reset_index(name="count")
        fig = px.bar(
            hourly, x="hour", y="count", color="is_fraud", barmode="group",
            title="Transactions by Hour", color_discrete_map={0: "#636EFA", 1: "#EF553B"},
        )
        st.plotly_chart(fig, width='stretch')

    eda3, eda4 = st.columns(2)
    with eda3:
        fig = px.box(df, x="is_fraud", y="distance_from_home", color="is_fraud",
                     title="Distance from Home", color_discrete_map={0: "#636EFA", 1: "#EF553B"})
        st.plotly_chart(fig, width='stretch')

    with eda4:
        cat_fraud = df.groupby("category")["is_fraud"].mean().reset_index()
        fig = px.bar(cat_fraud, x="category", y="is_fraud", title="Fraud Rate by Category",
                     labels={"is_fraud": "Fraud Rate"}, color="is_fraud", color_continuous_scale="Reds")
        st.plotly_chart(fig, width='stretch')

# ── Model training (only for synthetic schema) ───────────────────────────────
if is_synthetic_schema:
    st.header("🤖 Model Performance")

    @st.cache_data
    def run_model(data: pd.DataFrame, smote: bool):
        X, y = preprocess(data)
        return train_model(X, y, apply_smote=smote)

    with st.spinner("Training XGBoost model..."):
        results = run_model(df, apply_smote)

    # Metrics row
    m1, m2, m3, m4 = st.columns(4)
    report = results["classification_report"]
    m1.metric("ROC AUC", f"{results['roc_auc']:.4f}")
    m2.metric("PR AUC", f"{results['pr_auc']:.4f}")
    m3.metric("Precision (fraud)", f"{report['1']['precision']:.4f}")
    m4.metric("Recall (fraud)", f"{report['1']['recall']:.4f}")

    # Confusion matrix & feature importance
    res1, res2 = st.columns(2)

    with res1:
        cm = results["confusion_matrix"]
        fig = px.imshow(
            cm, text_auto=True, title="Confusion Matrix",
            labels=dict(x="Predicted", y="Actual", color="Count"),
            x=["Legitimate", "Fraud"], y=["Legitimate", "Fraud"],
            color_continuous_scale="Blues",
        )
        st.plotly_chart(fig, use_container_width=True)

    with res2:
        imp_df = pd.DataFrame({
            "feature": results["feature_names"],
            "importance": results["feature_importances"],
        }).sort_values("importance", ascending=True)
        fig = px.bar(imp_df, x="importance", y="feature", orientation="h",
                     title="Feature Importance", color="importance", color_continuous_scale="Viridis")
        st.plotly_chart(fig, use_container_width=True)

    # PR curve
    st.subheader("Precision-Recall Curve")
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=results["recall_curve"], y=results["precision_curve"],
                             mode="lines", name=f"PR (AUC={results['pr_auc']:.4f})"))
    fig.update_layout(xaxis_title="Recall", yaxis_title="Precision", height=400)
    st.plotly_chart(fig, use_container_width=True)

# ── FRAML Rules Engine ────────────────────────────────────────────────────────
if uploaded_file is not None:
    st.header("🛡️ FRAML Rules Evaluation")

    rule_instances = [RuleClass() for RuleClass in ALL_RULES]

    # Let user pick which rules to run
    rule_options = {r.rule_id + " – " + r.rule_name: r for r in rule_instances}
    selected_labels = st.multiselect(
        "Select rules to evaluate",
        options=list(rule_options.keys()),
        default=list(rule_options.keys()),
    )
    selected_rules = [rule_options[label] for label in selected_labels]

    if st.button("▶ Run selected rules") and selected_rules:
        # Convert DataFrame rows to Transaction objects once
        def _row_to_transaction(row):
            ts = row.get("transaction_timestamp", "")
            if isinstance(ts, str):
                try:
                    ts = datetime.fromisoformat(ts)
                except (ValueError, TypeError):
                    ts = datetime.now()
            return Transaction(
                transaction_id=str(row.get("transaction_id", "")),
                transaction_timestamp=ts,
                customer_id=int(row.get("customer_id", 0)),
                customer_account=str(row.get("customer_account", "")),
                channel=str(row.get("channel", "")),
                device_id=str(row.get("device_id", "")),
                amount=float(row.get("amount", 0)),
                currency=str(row.get("currency", "")),
                is_new_beneficiary=bool(row.get("is_new_beneficiary", False)),
                beneficiary_account=str(row.get("beneficiary_account", "")),
                entered_beneficiary_name=str(row.get("entered_beneficiary_name", "")),
                official_beneficiary_account_name=str(row.get("official_beneficiary_account_name", "")),
                customer_account_balance=float(row.get("customer_account_balance", 0)),
            )

        all_transactions = [_row_to_transaction(row) for _, row in df.iterrows()]
        # Build customer history lookup
        from collections import defaultdict
        customer_history_map = defaultdict(list)
        for t in all_transactions:
            customer_history_map[t.customer_id].append(t)

        results_rows = []
        progress = st.progress(0)
        for idx, tx in enumerate(all_transactions):
            customer_history = customer_history_map.get(tx.customer_id, [])
            for rule in selected_rules:
                result = rule.evaluate(tx, history=customer_history)
                if result.triggered:
                    results_rows.append({
                        "transaction_id": tx.transaction_id,
                        "rule_id": result.rule_id,
                        "rule_name": result.rule_name,
                        "severity": result.severity.name if result.severity else "",
                        "weight": result.weight,
                        "details": str(result.details),
                    })
            progress.progress((idx + 1) / len(all_transactions))
        progress.empty()

        if results_rows:
            alerts_df = pd.DataFrame(results_rows)
            st.subheader(f"🚨 {len(alerts_df)} alerts triggered")
            st.dataframe(alerts_df, use_container_width=True)

            # Summary by rule
            summary = alerts_df.groupby(["rule_id", "rule_name"]).size().reset_index(name="count")
            fig = px.bar(summary, x="rule_id", y="count", color="rule_name",
                         title="Alerts by Rule")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.success("✅ No rules triggered – all transactions passed.")

# ── Footer ───────────────────────────────────────────────────────────────────
st.markdown("---")
st.caption("Fraud Detection Dashboard • Built with Streamlit, XGBoost & scikit-learn")
