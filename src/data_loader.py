"""Data loading and preprocessing utilities for fraud detection."""

import pandas as pd
import numpy as np
from pathlib import Path
#from .transaction import Transaction
from src.transaction import Transaction



def load_data(file_path: str | Path) -> list[Transaction]:
    """Load transaction data from a CSV file into a list of Transaction objects."""
    csv_path = Path(file_path)

    try:
        df = pd.read_csv(csv_path)
        transactions: list[Transaction] = []
        for record in df.to_dict(orient="records"):
            transactions.append(Transaction(**record))
        if transactions:
            print("Top 10 transactions:")
            for t in transactions[:10]:
                print(t)
        else:
            print("No transactions loaded.")
        return transactions

    except Exception as e:
        raise RuntimeError(f"Failed to read CSV at {csv_path}: {e}") from e



def generate_synthetic_data(n_samples: int = 10_000, fraud_ratio: float = 0.02, seed: int = 42) -> pd.DataFrame:
    """Generate synthetic credit card transaction data for demo purposes."""
    rng = np.random.default_rng(seed)

    n_fraud = int(n_samples * fraud_ratio)
    n_legit = n_samples - n_fraud

    legit = pd.DataFrame({
        "amount": rng.exponential(scale=80, size=n_legit).round(2),
        "hour": rng.integers(0, 24, size=n_legit),
        "day_of_week": rng.integers(0, 7, size=n_legit),
        "category": rng.choice(["grocery", "gas", "online", "restaurant", "travel", "entertainment"], size=n_legit),
        "merchant_risk_score": rng.uniform(0, 0.3, size=n_legit).round(4),
        "distance_from_home": rng.exponential(scale=10, size=n_legit).round(2),
        "is_international": rng.choice([0, 1], p=[0.95, 0.05], size=n_legit),
        "num_transactions_last_hour": rng.poisson(1.5, size=n_legit),
        "is_fraud": 0,
    })

    fraud = pd.DataFrame({
        "amount": rng.exponential(scale=500, size=n_fraud).round(2),
        "hour": rng.choice([0, 1, 2, 3, 4, 23], size=n_fraud),
        "day_of_week": rng.integers(0, 7, size=n_fraud),
        "category": rng.choice(["online", "travel", "entertainment"], size=n_fraud),
        "merchant_risk_score": rng.uniform(0.4, 1.0, size=n_fraud).round(4),
        "distance_from_home": rng.exponential(scale=80, size=n_fraud).round(2),
        "is_international": rng.choice([0, 1], p=[0.5, 0.5], size=n_fraud),
        "num_transactions_last_hour": rng.poisson(5, size=n_fraud),
        "is_fraud": 1,
    })

    df = pd.concat([legit, fraud], ignore_index=True).sample(frac=1, random_state=seed).reset_index(drop=True)
    df["transaction_id"] = range(1, len(df) + 1)
    return df


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


def preprocess(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """Preprocess dataframe: encode categoricals, return X and y."""
    df = df.copy()
    y = df.pop("is_fraud")
    df = df.drop(columns=["transaction_id"], errors="ignore")
    df = pd.get_dummies(df, columns=["category"], drop_first=True)
    return df, y
