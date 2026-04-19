"""Streamlit dashboard for the Fraud Detection project."""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st
import pandas as pd

from presentation.alert_dashboard import render_alert_dashboard
from presentation.data_quality_dashboard import render_data_quality, render_data_exploration
from presentation.graph_dashboard import render_graph_dashboard
from presentation.rules_explanation_panel import render_rules_explanation_panel
from infrastructure.graph.provider import Neo4jGraphProvider
from infrastructure.csv_exporter import CsvResultExporter
from application.workflow_runner import WorkflowRunner


# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(page_title="MegaFraudDetector9000+", page_icon="👮", layout="wide")
st.sidebar.image(str(Path(__file__).resolve().parent / "logo.png"))
st.title("🔍 Fraud Detection Dashboard")

# ── Sidebar controls ────────────────────────────────────────────────────────
st.sidebar.header("⚙️ Settings")
view = st.sidebar.radio(
    "Navigate",
    options=["Dashboards", "Rules Explanation"],
    index=0,
)

project_root = Path(__file__).resolve().parent.parent
rules_candidates = [
    project_root / "HACKATHON_FRAML_RULES.xlsx",
    project_root / "test" / "4. HACKATHON_FRAML_RULES.xlsx",
]
rules_file = next((p for p in rules_candidates if p.exists()), rules_candidates[-1])

if view == "Rules Explanation":
    render_rules_explanation_panel(rules_file)
    st.stop()

uploaded_file = st.sidebar.file_uploader("Upload a transactions CSV", type=["csv"])

# ── Data loading ─────────────────────────────────────────────────────────────
if uploaded_file is None:
    st.info("📂 Upload a CSV file to get started.")
    st.stop()

df = pd.read_csv(uploaded_file)

# Cache workflow result so filter changes don't re-run the expensive evaluation
_cache_key = f"wf_result_{uploaded_file.name}_{uploaded_file.size}"
if _cache_key not in st.session_state:
    with st.spinner("Processing transactions…"):
        graph = Neo4jGraphProvider()
        exporter = CsvResultExporter()
        runner = WorkflowRunner(graph_repository=graph, result_exporter=exporter)
        st.session_state[_cache_key] = runner.run_process_list(df)
wf_result = st.session_state[_cache_key]

# ── Risk assessments download ────────────────────────────────────────────────
if wf_result.risk_csv:
    st.sidebar.download_button(
        label="📥 Download Risk Assessments",
        data=wf_result.risk_csv,
        file_name="MegaFraudDetector9000Plus_risk_assessments.csv",
        mime="text/csv",
    )

# ── Tabs ─────────────────────────────────────────────────────────────────────
tab_alerts, tab_graph, tab_explore, tab_quality = st.tabs([
    "🛡️ Alert Dashboard",
    "🕸️ Graph Intelligence",
    "📈 Data Exploration",
    "🔎 Data Quality",
])

with tab_alerts:
    render_alert_dashboard(wf_result)

with tab_graph:
    try:
        with Neo4jGraphProvider() as graph:
            render_graph_dashboard(graph)
    except Exception as e:
        import traceback
        st.info(f"⚠️ Graph database not available – skipping graph dashboard.\n\n`{e}`")
        st.code(traceback.format_exc())

with tab_explore:
    render_data_exploration(df)

with tab_quality:
    render_data_quality(df)

# ── Footer ───────────────────────────────────────────────────────────────────
st.markdown("---")
st.caption("Fraud Detection Dashboard • Built with Streamlit")
