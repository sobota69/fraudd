"""Model training and evaluation for fraud detection."""

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    roc_auc_score,
    precision_recall_curve,
    auc,
)
from sklearn.preprocessing import StandardScaler
from imblearn.over_sampling import SMOTE
from xgboost import XGBClassifier


def train_model(
    X: pd.DataFrame,
    y: pd.Series,
    test_size: float = 0.2,
    apply_smote: bool = True,
    seed: int = 42,
) -> dict:
    """Train an XGBoost classifier and return model + evaluation artifacts."""
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=seed, stratify=y
    )

    scaler = StandardScaler()
    X_train_scaled = pd.DataFrame(scaler.fit_transform(X_train), columns=X_train.columns)
    X_test_scaled = pd.DataFrame(scaler.transform(X_test), columns=X_test.columns)

    if apply_smote:
        smote = SMOTE(random_state=seed)
        X_train_scaled, y_train = smote.fit_resample(X_train_scaled, y_train)

    model = XGBClassifier(
        n_estimators=200,
        max_depth=6,
        learning_rate=0.1,
        scale_pos_weight=1 if apply_smote else (y_train == 0).sum() / max((y_train == 1).sum(), 1),
        use_label_encoder=False,
        eval_metric="logloss",
        random_state=seed,
    )
    model.fit(X_train_scaled, y_train)

    y_pred = model.predict(X_test_scaled)
    y_proba = model.predict_proba(X_test_scaled)[:, 1]

    precision, recall, thresholds = precision_recall_curve(y_test, y_proba)

    return {
        "model": model,
        "scaler": scaler,
        "X_test": X_test_scaled,
        "y_test": y_test,
        "y_pred": y_pred,
        "y_proba": y_proba,
        "classification_report": classification_report(y_test, y_pred, output_dict=True),
        "confusion_matrix": confusion_matrix(y_test, y_pred),
        "roc_auc": roc_auc_score(y_test, y_proba),
        "pr_auc": auc(recall, precision),
        "precision_curve": precision,
        "recall_curve": recall,
        "feature_names": list(X.columns),
        "feature_importances": model.feature_importances_,
    }
