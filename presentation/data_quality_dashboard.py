"""Other metrics dashboard assembler – file analysis, visualisations & data overview."""

from __future__ import annotations

from bisect import bisect_left
from collections import Counter, deque

import pandas as pd
import plotly.express as px
import streamlit as st

from infrastructure.data_loader import analyse_dataframe


def render_data_quality(df: pd.DataFrame) -> None:
    """Render uploaded file analysis and data quality checks."""

    st.header("🔎 Uploaded File Analysis")
    analysis = analyse_dataframe(df)

    a1, a2, a3, a4 = st.columns(4)
    a1.metric("Rows", f"{analysis['shape'][0]:,}")
    a2.metric("Columns", f"{analysis['shape'][1]:,}")
    a3.metric("Duplicate rows", f"{analysis['duplicates']:,}")
    total_cells = analysis["shape"][0] * analysis["shape"][1]
    miss_pct = (analysis["total_missing"] / total_cells * 100) if total_cells else 0
    a4.metric("Missing values", f"{analysis['total_missing']:,} ({miss_pct:.1f}%)")

    with st.expander("Column data types"):
        st.json(analysis["dtypes"])

    if not analysis["numeric_stats"].empty:
        with st.expander("Numeric column statistics", expanded=True):
            st.dataframe(analysis["numeric_stats"].style.format(precision=2), width='stretch')

    if not analysis["categorical_stats"].empty:
        with st.expander("Categorical column statistics", expanded=True):
            st.dataframe(analysis["categorical_stats"], width='stretch')

    if not analysis["correlations"].empty:
        with st.expander("Correlation heatmap", expanded=True):
            fig = px.imshow(
                analysis["correlations"], text_auto=".2f",
                title="Feature Correlations", color_continuous_scale="RdBu_r",
                zmin=-1, zmax=1,
            )
            st.plotly_chart(fig, width='stretch')

    missing_series = df.isnull().sum()
    missing_series = missing_series[missing_series > 0]
    if not missing_series.empty:
        with st.expander("Missing values per column"):
            fig = px.bar(x=missing_series.index, y=missing_series.values,
                         labels={"x": "Column", "y": "Missing count"},
                         title="Missing Values by Column")
            st.plotly_chart(fig, width='stretch')

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

    ts_col = None
    if datetime_cols:
        ts_col = datetime_cols[0]
        df["_parsed_ts"] = pd.to_datetime(df[ts_col], errors="coerce")

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

    # ── Rule-violation snapshots aligned with implemented rules ─────────────
    st.subheader("🧪 Rule Violations (Aligned to Rule Logic)")

    def _country_from_iban(iban: str) -> str | None:
        iban_str = str(iban).strip()
        if len(iban_str) >= 2 and iban_str[:2].isalpha():
            return iban_str[:2].upper()
        return None

    def _smallest_90pct_window(hour_counts: Counter[int]) -> tuple[int, int]:
        total = sum(hour_counts.values())
        if total <= 0:
            return 0, 24
        target = total * 0.90

        best_start, best_size = 0, 24
        for start in range(24):
            covered = 0
            for size in range(1, 25):
                h = (start + size - 1) % 24
                covered += hour_counts.get(h, 0)
                if covered >= target:
                    if size < best_size:
                        best_start, best_size = start, size
                    break
        return best_start, best_size

    # R1/R2/R3 CoP name similarity (computed exactly as in CopGroup)
    if {"entered_beneficiary_name", "official_beneficiary_account_name"}.issubset(df.columns):
        import unicodedata, re
        from difflib import SequenceMatcher

        _TITLES = frozenset({
            "mr", "mrs", "ms", "dr", "prof", "sir", "jr", "sr",
            "sa", "sp", "z", "o", "oo", "pan", "pani", "univ",
        })

        def _cop_normalise(name: str) -> str:
            nfkd = unicodedata.normalize("NFKD", str(name))
            ascii_only = "".join(ch for ch in nfkd if not unicodedata.combining(ch))
            return re.sub(r"[^a-z0-9\s]", "", ascii_only.lower()).strip()

        def _cop_tokenise(text: str) -> list[str]:
            return [t for t in text.split() if len(t) > 1 and t not in _TITLES]

        def _cop_similarity(entered: str, official: str) -> float:
            ta = _cop_tokenise(_cop_normalise(entered))
            tb = _cop_tokenise(_cop_normalise(official))
            if not ta and not tb:
                return 1.0
            if not ta or not tb:
                return 0.0
            total = sum(max(SequenceMatcher(None, t, b).ratio() for b in tb) for t in ta)
            return total / max(len(ta), len(tb))

        cop_df = df[[c for c in [
            "transaction_id", "transaction_timestamp", "customer_id",
            "entered_beneficiary_name", "official_beneficiary_account_name",
            "amount", "currency", "is_new_beneficiary",
        ] if c in df.columns]].copy()
        cop_df["_similarity_pct"] = cop_df.apply(
            lambda r: round(_cop_similarity(
                r.get("entered_beneficiary_name", "") or "",
                r.get("official_beneficiary_account_name", "") or "",
            ) * 100, 2), axis=1
        )

        r1_df = cop_df[cop_df["_similarity_pct"] < 80]
        r2_df = cop_df[(cop_df["_similarity_pct"] >= 80) & (cop_df["_similarity_pct"] < 90)]
        if "is_new_beneficiary" in cop_df.columns:
            is_new_bool = cop_df["is_new_beneficiary"].astype(str).str.lower().isin(["true", "1", "yes"])
            r3_df = cop_df[is_new_bool & (cop_df["_similarity_pct"] < 90)]
        else:
            r3_df = pd.DataFrame()

        cop_display_cols = [c for c in [
            "transaction_id", "transaction_timestamp", "customer_id",
            "entered_beneficiary_name", "official_beneficiary_account_name",
            "_similarity_pct", "amount", "currency",
        ] if c in cop_df.columns]

        if len(r1_df):
            with st.expander(f"R1 CoP Name Mismatch – Hard Fail (similarity < 80%) ({len(r1_df):,})"):
                st.dataframe(r1_df[cop_display_cols].head(200), width='stretch')
        if len(r2_df):
            with st.expander(f"R2 CoP Name Mismatch – Soft Warning (80% ≤ similarity < 90%) ({len(r2_df):,})"):
                st.dataframe(r2_df[cop_display_cols].head(200), width='stretch')
        if len(r3_df):
            r3_cols = [c for c in cop_display_cols + ["is_new_beneficiary"] if c in cop_df.columns]
            with st.expander(f"R3 New Beneficiary + CoP Mismatch (new ben & similarity < 90%) ({len(r3_df):,})"):
                st.dataframe(r3_df[r3_cols].head(200), width='stretch')

    # Need core columns to align with actual rule evaluation flow
    has_core = {"customer_id", "amount"}.issubset(df.columns)
    ts_source = "transaction_timestamp" if "transaction_timestamp" in df.columns else None
    if not ts_source and "_parsed_ts" in df.columns:
        ts_source = "_parsed_ts"

    if has_core and ts_source:
        ordered = df.copy()
        ordered["_ts"] = pd.to_datetime(ordered[ts_source], errors="coerce")
        ordered["_amount_num"] = pd.to_numeric(ordered["amount"], errors="coerce")
        ordered = ordered.dropna(subset=["_ts", "_amount_num"]).sort_values(["customer_id", "_ts"]).copy()
        ordered["_hour_for_rule"] = ordered["_ts"].dt.hour

        # R10: dest country unseen in customer history AND amount > 15000
        if "beneficiary_account" in ordered.columns:
            ordered["_r10_cross_border"] = False
            for _, g in ordered.groupby("customer_id", sort=False):
                seen: set[str] = set()
                for idx, row in g.iterrows():
                    dest = _country_from_iban(row.get("beneficiary_account", ""))
                    amt = float(row["_amount_num"])
                    if dest and amt > 15000 and dest not in seen:
                        ordered.at[idx, "_r10_cross_border"] = True
                    if dest:
                        seen.add(dest)

            n_r10 = int(ordered["_r10_cross_border"].sum())
            if n_r10 > 0:
                with st.expander(f"R10 Cross-Border Anomaly (>15,000 and unseen destination country) ({n_r10:,})"):
                    cols = [c for c in [
                        "transaction_id", "transaction_timestamp", "customer_id", "beneficiary_account", "amount", "currency",
                    ] if c in ordered.columns]
                    st.dataframe(ordered[ordered["_r10_cross_border"]][cols].head(200), width='stretch')

        # R13: unusual hour outside smallest 90% hour window; min 10 history txs
        ordered["_r13_unusual_hour"] = False
        ordered["_r13_history_count"] = 0
        ordered["_r13_window_start"] = pd.NA
        ordered["_r13_window_size"] = pd.NA
        for _, g in ordered.groupby("customer_id", sort=False):
            hour_counts: Counter[int] = Counter()
            history_count = 0
            for idx, row in g.iterrows():
                cur_hour = int(row["_hour_for_rule"])
                if history_count >= 10:
                    start, size = _smallest_90pct_window(hour_counts)
                    in_window = ((cur_hour - start) % 24) < size
                    if not in_window:
                        ordered.at[idx, "_r13_unusual_hour"] = True
                    ordered.at[idx, "_r13_history_count"] = history_count
                    ordered.at[idx, "_r13_window_start"] = start
                    ordered.at[idx, "_r13_window_size"] = size

                hour_counts[cur_hour] += 1
                history_count += 1

        n_r13 = int(ordered["_r13_unusual_hour"].sum())
        if n_r13 > 0:
            with st.expander(f"R13 Unusual Hour (outside customer's 90% activity window; history ≥10) ({n_r13:,})"):
                cols = [c for c in [
                    "transaction_id", "transaction_timestamp", "customer_id", "amount", "currency", "channel",
                    "_r13_history_count", "_r13_window_start", "_r13_window_size",
                ] if c in ordered.columns]
                st.dataframe(ordered[ordered["_r13_unusual_hour"]][cols].head(200), width='stretch')

        # R18: at least 3 round-amount txs (multiple of 10) within trailing 48h
        ordered["_r18_round_amounts"] = False
        ordered["_r18_round_count_48h"] = 0
        for _, g in ordered.groupby("customer_id", sort=False):
            dq: deque[pd.Timestamp] = deque()
            for idx, row in g.iterrows():
                ts = row["_ts"]
                window_start = ts - pd.Timedelta(hours=48)
                while dq and dq[0] < window_start:
                    dq.popleft()

                amt = float(row["_amount_num"])
                current_is_round = amt > 0 and amt % 10 == 0
                total = len(dq) + (1 if current_is_round else 0)
                ordered.at[idx, "_r18_round_count_48h"] = total
                if total >= 3:
                    ordered.at[idx, "_r18_round_amounts"] = True

                if current_is_round:
                    dq.append(ts)

        n_r18 = int(ordered["_r18_round_amounts"].sum())
        if n_r18 > 0:
            with st.expander(f"R18 Round Amounts Anomaly (≥3 multiples-of-10 in 48h) ({n_r18:,})"):
                cols = [c for c in [
                    "transaction_id", "transaction_timestamp", "customer_id", "amount", "currency", "_r18_round_count_48h",
                ] if c in ordered.columns]
                st.dataframe(ordered[ordered["_r18_round_amounts"]][cols].head(200), width='stretch')

        # R21: rapid account emptying; drop ratio > 70% within 1h window
        if {"customer_account", "customer_account_balance"}.issubset(ordered.columns):
            ordered["_r21_rapid_emptying"] = False
            ordered["_r21_drop_ratio"] = pd.NA
            bal_num = pd.to_numeric(ordered["customer_account_balance"], errors="coerce")
            ordered["_balance_num"] = bal_num

            for _, g in ordered.groupby("customer_id", sort=False):
                acc_hist: dict[str, list[tuple[pd.Timestamp, float]]] = {}

                for idx, row in g.iterrows():
                    ts = row["_ts"]
                    acc = str(row.get("customer_account", ""))
                    amount = float(row["_amount_num"])
                    balance_after = row.get("_balance_num")

                    if pd.isna(balance_after):
                        continue
                    balance_after = float(balance_after)

                    window_start = ts - pd.Timedelta(hours=1)
                    hist = acc_hist.get(acc, [])

                    balance_before = None
                    if hist:
                        ts_list = [t for t, _ in hist]
                        boundary = bisect_left(ts_list, window_start)
                        if boundary - 1 >= 0:
                            balance_before = hist[boundary - 1][1]

                    if balance_before is None:
                        balance_before = balance_after + amount

                    if balance_before > 0:
                        drop_ratio = (balance_before - balance_after) / balance_before
                        ordered.at[idx, "_r21_drop_ratio"] = round(drop_ratio, 4)
                        if drop_ratio > 0.70:
                            ordered.at[idx, "_r21_rapid_emptying"] = True

                    hist.append((ts, balance_after))
                    acc_hist[acc] = hist

            n_r21 = int(ordered["_r21_rapid_emptying"].fillna(False).sum())
            if n_r21 > 0:
                with st.expander(f"R21 Rapid Account Emptying (drop ratio >70% in 1h) ({n_r21:,})"):
                    cols = [c for c in [
                        "transaction_id", "transaction_timestamp", "customer_id", "customer_account",
                        "customer_account_balance", "amount", "currency", "_r21_drop_ratio",
                    ] if c in ordered.columns]
                    st.dataframe(ordered[ordered["_r21_rapid_emptying"].fillna(False)][cols].head(200), width='stretch')

        # Merge rule flags back to df by index for potential downstream use
        for c in [
            "_r10_cross_border", "_r13_unusual_hour", "_r13_history_count", "_r13_window_start", "_r13_window_size",
            "_r18_round_amounts", "_r18_round_count_48h", "_r21_rapid_emptying", "_r21_drop_ratio",
        ]:
            if c in ordered.columns:
                df.loc[ordered.index, c] = ordered[c]

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

    df.drop(columns=[c for c in df.columns if c.startswith("_")], inplace=True, errors="ignore")
