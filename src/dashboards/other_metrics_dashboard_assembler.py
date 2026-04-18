"""Other metrics dashboard assembler – file analysis, visualisations & data overview."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from src.data_loader import analyse_dataframe


def render_data_quality(df: pd.DataFrame) -> None:
    """Render uploaded file analysis and data quality checks."""

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

    # Data overview
    st.markdown("---")
    st.header("📊 Data Overview")
    col1, col2 = st.columns(2)
    col1.metric("Total transactions", f"{len(df):,}")
    col2.metric("Columns", f"{len(df.columns):,}")

    with st.expander("Show raw data sample"):
        st.dataframe(df.head(100))


def render_data_exploration(df: pd.DataFrame) -> None:
    """Render smart visualisations for transaction data exploration."""

    st.header("📈 Data Exploration")

    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    datetime_cols = []
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
