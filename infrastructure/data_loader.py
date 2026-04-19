"""Data analysis utilities for fraud detection."""

import pandas as pd
import numpy as np


def analyse_dataframe(df: pd.DataFrame) -> dict:
    """Return a summary analysis dict for a given dataframe."""
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    categorical_cols = df.select_dtypes(include=["object", "category"]).columns.tolist()

    stats = df[numeric_cols].describe().T if numeric_cols else pd.DataFrame()
    stats["missing"] = df[numeric_cols].isnull().sum() if numeric_cols else []
    stats["missing_%"] = (df[numeric_cols].isnull().mean() * 100).round(2) if numeric_cols else []

    cat_stats = []
    for c in categorical_cols:
        cat_stats.append({
            "column": c,
            "unique": df[c].nunique(),
            "top": df[c].mode().iloc[0] if not df[c].mode().empty else None,
            "missing": int(df[c].isnull().sum()),
            "missing_%": round(df[c].isnull().mean() * 100, 2),
        })

    correlations = df[numeric_cols].corr() if len(numeric_cols) > 1 else pd.DataFrame()

    duplicates = int(df.duplicated().sum())

    return {
        "shape": df.shape,
        "numeric_stats": stats,
        "categorical_stats": pd.DataFrame(cat_stats) if cat_stats else pd.DataFrame(),
        "correlations": correlations,
        "duplicates": duplicates,
        "dtypes": df.dtypes.astype(str).to_dict(),
        "total_missing": int(df.isnull().sum().sum()),
    }
