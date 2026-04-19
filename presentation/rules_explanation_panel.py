"""Rules explanation panel loaded from the FRAML rules Excel file."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st


def _norm(name: str) -> str:
    return " ".join(str(name).replace("\n", " ").split()).strip().lower()


_HARDCODED_FALLBACKS: dict[str, dict[str, str]] = {
    "R10": {
        "algorithm": "Compare beneficiary country in current transaction against customer's historical baseline. Trigger when destination country is rare/new and amount is materially higher than normal.",
        "examples": "Example: Customer usually pays domestic beneficiaries but suddenly sends a high-value transfer to a first-time foreign beneficiary.",
        "transaction_data_needed": "transaction_timestamp, customer_id, amount, beneficiary_country, customer_account",
    },
    "R13": {
        "algorithm": "Compute customer activity histogram by hour-of-day and weekday. Trigger for transactions in low-frequency hours, especially when combined with elevated amount or new beneficiary.",
        "examples": "Example: A customer who usually transacts 08:00–20:00 performs a high-value transfer at 03:15.",
        "transaction_data_needed": "transaction_timestamp, customer_id, amount, transaction_hour_of_day, transaction_day_of_week",
    },
    "R18": {
        "algorithm": "Identify suspiciously round amounts and compare with customer distribution of historical transfer amounts. Trigger when roundness plus context indicates anomaly.",
        "examples": "Example: Repeated transfers of exactly 1000, 2000, 5000 with no matching historical behavior.",
        "transaction_data_needed": "amount, customer_id, transaction_timestamp, customer_account",
    },
    "R21": {
        "algorithm": "Track rapid drop in account balance after one or more outgoing transfers. Trigger when transfer sequence materially empties account within a short time window.",
        "examples": "Example: Balance falls from 12,000 to 250 within minutes after consecutive outbound transfers.",
        "transaction_data_needed": "customer_account_balance, amount, customer_id, transaction_timestamp, customer_account",
    },
}


def _infer_tx_fields(row: pd.Series) -> str:
    text = " ".join(
        str(row.get(c, "")) for c in ["rule_name", "business_description", "threshold_condition", "algorithm"]
    ).lower()

    fields: list[str] = ["transaction_id", "customer_id", "amount", "currency", "transaction_timestamp"]

    if any(k in text for k in ["beneficiary", "cop", "name mismatch", "recipient"]):
        fields.extend([
            "is_new_beneficiary",
            "beneficiary_account",
            "entered_beneficiary_name",
            "official_beneficiary_account_name",
        ])
    if any(k in text for k in ["country", "cross-border", "cross border", "international"]):
        fields.append("beneficiary_country")
    if any(k in text for k in ["hour", "time", "night", "weekday", "day of week"]):
        fields.extend(["transaction_hour_of_day", "transaction_day_of_week"])
    if any(k in text for k in ["balance", "emptying", "drain", "rapid"]):
        fields.append("customer_account_balance")
    if any(k in text for k in ["device", "channel"]):
        fields.extend(["device_id", "channel"])

    # de-duplicate while preserving order
    seen: set[str] = set()
    ordered = [f for f in fields if not (f in seen or seen.add(f))]
    return ", ".join(ordered)


def _fill_missing_rule_fields(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    for idx, row in out.iterrows():
        rid = str(row.get("rule_id", "")).strip().upper()
        fallback = _HARDCODED_FALLBACKS.get(rid, {})

        algo = str(row.get("algorithm", "")).strip()
        if not algo:
            out.at[idx, "algorithm"] = fallback.get(
                "algorithm",
                "Evaluate the rule condition against current transaction and relevant customer history; trigger when threshold is exceeded.",
            )

        ex = str(row.get("examples", "")).strip()
        if not ex:
            rule_name = str(row.get("rule_name", "this rule")).strip()
            out.at[idx, "examples"] = fallback.get(
                "examples",
                f"Example scenario: a transaction pattern matches {rule_name} based on the configured threshold/condition.",
            )

        tx_needed = str(row.get("transaction_data_needed", "")).strip()
        if not tx_needed:
            out.at[idx, "transaction_data_needed"] = fallback.get("transaction_data_needed", _infer_tx_fields(row))

    return out


@st.cache_data(show_spinner=False)
def _load_rules(path_str: str) -> pd.DataFrame:
    path = Path(path_str)
    xls = pd.ExcelFile(path)
    sheet = xls.sheet_names[0]
    raw = pd.read_excel(path, sheet_name=sheet)

    rename_map: dict[str, str] = {}
    for col in raw.columns:
        key = _norm(col)
        if key == "rule id":
            rename_map[col] = "rule_id"
        elif key == "rule category":
            rename_map[col] = "rule_category"
        elif key == "rule":
            rename_map[col] = "rule_name"
        elif key.startswith("business description"):
            rename_map[col] = "business_description"
        elif key in {"mandatory /optional", "mandatory/optional", "mandatory / optional"}:
            rename_map[col] = "mandatory_optional"
        elif key.startswith("severity"):
            rename_map[col] = "severity"
        elif key == "weight":
            rename_map[col] = "weight"
        elif key.startswith("difficulty"):
            rename_map[col] = "difficulty"
        elif key == "threshold / condition":
            rename_map[col] = "threshold_condition"
        elif key.startswith("algorythm") or key.startswith("algorithm"):
            rename_map[col] = "algorithm"
        elif key == "examples":
            rename_map[col] = "examples"
        elif key == "transaction data needed":
            rename_map[col] = "transaction_data_needed"

    df = raw.rename(columns=rename_map).copy()

    for c in [
        "rule_id",
        "rule_category",
        "rule_name",
        "business_description",
        "severity",
        "threshold_condition",
        "algorithm",
        "examples",
        "transaction_data_needed",
    ]:
        if c in df.columns:
            df[c] = df[c].fillna("").astype(str).str.strip()

    if "weight" in df.columns:
        df["weight"] = pd.to_numeric(df["weight"], errors="coerce")

    if "rule_id" in df.columns:
        df = df[df["rule_id"].astype(str).str.strip() != ""]

    df = _fill_missing_rule_fields(df)
    return df.reset_index(drop=True)


def render_rules_explanation_panel(rules_file: Path) -> None:
    st.header("📘 Rules Explanation Panel")
    st.caption("Source: HACKATHON_FRAML_RULES.xlsx")

    if not rules_file.exists():
        st.error(f"Rules file not found: {rules_file}")
        return

    df = _load_rules(str(rules_file))
    if df.empty:
        st.warning("No rules were found in the Excel file.")
        return

    m1, m2, m3 = st.columns(3)
    m1.metric("Total Rules", f"{len(df):,}")
    m2.metric("Categories", f"{df.get('rule_category', pd.Series(dtype=str)).nunique():,}")
    m3.metric("Severity Levels", f"{df.get('severity', pd.Series(dtype=str)).nunique():,}")

    c1, c2 = st.columns(2)
    with c1:
        categories = sorted([x for x in df.get("rule_category", pd.Series(dtype=str)).dropna().unique().tolist() if x])
        cat_filter = st.multiselect("Rule category", options=categories, default=categories)
    with c2:
        severities = sorted([x for x in df.get("severity", pd.Series(dtype=str)).dropna().unique().tolist() if x])
        sev_filter = st.multiselect("Severity", options=severities, default=severities)

    search = st.text_input("Search (rule id, name, description, threshold)")

    filtered = df.copy()
    if cat_filter and "rule_category" in filtered.columns:
        filtered = filtered[filtered["rule_category"].isin(cat_filter)]
    if sev_filter and "severity" in filtered.columns:
        filtered = filtered[filtered["severity"].isin(sev_filter)]

    if search.strip():
        q = search.strip().lower()
        text_cols = [c for c in ["rule_id", "rule_name", "business_description", "threshold_condition"] if c in filtered.columns]
        mask = filtered[text_cols].fillna("").astype(str).apply(lambda col: col.str.lower().str.contains(q, regex=False))
        filtered = filtered[mask.any(axis=1)]

    st.markdown(f"**Showing {len(filtered):,} of {len(df):,} rules**")

    summary_cols = [c for c in ["rule_id", "rule_name", "rule_category", "severity", "weight"] if c in filtered.columns]
    if summary_cols:
        st.dataframe(filtered.reset_index(drop=True)[summary_cols], width='stretch', height=280)

    st.markdown("---")
    st.subheader("Rule Details")

    for _, row in filtered.iterrows():
        rid = row.get("rule_id", "")
        rname = row.get("rule_name", "")
        header = f"{rid} — {rname}" if rid else str(rname)

        with st.expander(header, expanded=False):
            meta_cols = st.columns(3)
            meta_cols[0].markdown(f"**Category:** {row.get('rule_category', '')}")
            meta_cols[1].markdown(f"**Severity:** {row.get('severity', '')}")
            meta_cols[2].markdown(f"**Weight:** {row.get('weight', '')}")

            st.markdown("**Business Description**")
            st.write(row.get("business_description", ""))

            st.markdown("**Threshold / Condition**")
            st.write(row.get("threshold_condition", ""))

            st.markdown("**Algorithm**")
            st.write(row.get("algorithm", ""))

            st.markdown("**Transaction Data Needed**")
            st.write(row.get("transaction_data_needed", ""))

            st.markdown("**Examples**")
            st.write(row.get("examples", ""))
