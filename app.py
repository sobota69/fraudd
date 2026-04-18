"""Streamlit dashboard for the Fraud Detection project."""

import streamlit as st
import pandas as pd

from src.dashboard_assembler import render_alert_dashboard
from src.other_metrics_dashboard_assembler import render_other_metrics
from src.workflow_runner import WorkflowRunner


# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(page_title="Fraud Detection Dashboard", page_icon="🔍", layout="wide")
st.title("🔍 Fraud Detection Dashboard")

# ── Sidebar controls ────────────────────────────────────────────────────────
st.sidebar.header("⚙️ Settings")
uploaded_file = st.sidebar.file_uploader("Upload a transactions CSV", type=["csv"])

# ── Data loading ─────────────────────────────────────────────────────────────
if uploaded_file is None:
    st.info("👈 Upload a CSV file to get started.")
    st.stop()

df = pd.read_csv(uploaded_file)

# Cache workflow result so filter changes don't re-run the expensive evaluation
_cache_key = f"wf_result_{uploaded_file.name}_{uploaded_file.size}"
if _cache_key not in st.session_state:
    with st.spinner("Processing transactions…"):
        st.session_state[_cache_key] = WorkflowRunner().run_process_list(df)
wf_result = st.session_state[_cache_key]

# ── FRAML Alert Dashboard (top of page) ──────────────────────────────────────
render_alert_dashboard(wf_result)

# ── Other metrics & visualisations ───────────────────────────────────────────
render_other_metrics(df)

# ── Footer ───────────────────────────────────────────────────────────────────
st.markdown("---")
st.caption("Fraud Detection Dashboard • Built with Streamlit, XGBoost & scikit-learn")
