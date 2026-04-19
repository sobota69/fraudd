"""Other metrics dashboard assembler – file analysis, visualisations & data overview."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from infrastructure.data_loader import analyse_dataframe
from presentation.data_exploration_dashboard import render_data_exploration


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

