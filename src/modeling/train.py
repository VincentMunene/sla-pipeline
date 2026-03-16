import pandas as pd
import numpy as np
from pathlib import Path
import joblib

from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    confusion_matrix,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
    accuracy_score,
)


def prepare_train_test_data(df: pd.DataFrame, target_col: str = "sla_breached"):
    df = df.copy()

    # Drop rows without target just in case
    df = df[df[target_col].notna()].copy()

    X = df.drop(columns=[target_col])
    y = df[target_col].astype(int)

    # Identify feature types
    categorical_cols = X.select_dtypes(include=["object"]).columns.tolist()
    numeric_cols = X.select_dtypes(include=[np.number, "bool"]).columns.tolist()

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42,
        stratify=y,
    )

    return X_train, X_test, y_train, y_test, categorical_cols, numeric_cols


def build_preprocessor(categorical_cols, numeric_cols):
    numeric_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )

    categorical_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore")),
        ]
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, numeric_cols),
            ("cat", categorical_transformer, categorical_cols),
        ]
    )

    return preprocessor


def evaluate_model(name, model_pipeline, X_train, X_test, y_train, y_test):
    model_pipeline.fit(X_train, y_train)

    y_pred = model_pipeline.predict(X_test)

    if hasattr(model_pipeline, "predict_proba"):
        y_prob = model_pipeline.predict_proba(X_test)[:, 1]
    else:
        y_prob = None

    metrics = {
        "model_name": name,
        "accuracy": accuracy_score(y_test, y_pred),
        "precision": precision_score(y_test, y_pred, zero_division=0),
        "recall": recall_score(y_test, y_pred, zero_division=0),
        "f1": f1_score(y_test, y_pred, zero_division=0),
        "roc_auc": roc_auc_score(y_test, y_prob) if y_prob is not None else np.nan,
    }

    cm = confusion_matrix(y_test, y_pred)

    return model_pipeline, metrics, cm


def train_and_compare_models(df: pd.DataFrame, models_dir: Path):
    X_train, X_test, y_train, y_test, categorical_cols, numeric_cols = prepare_train_test_data(df)

    preprocessor = build_preprocessor(categorical_cols, numeric_cols)

    logistic_pipeline = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("classifier", LogisticRegression(max_iter=1000, class_weight="balanced")),
        ]
    )

    rf_pipeline = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("classifier", RandomForestClassifier(
                n_estimators=200,
                random_state=42,
                class_weight="balanced_subsample",
                n_jobs=-1,
            )),
        ]
    )

    results = []
    fitted_models = {}

    for name, pipeline in [
        ("Logistic Regression", logistic_pipeline),
        ("Random Forest", rf_pipeline),
    ]:
        model, metrics, cm = evaluate_model(name, pipeline, X_train, X_test, y_train, y_test)
        results.append(metrics)
        fitted_models[name] = {
            "model": model,
            "confusion_matrix": cm,
        }

    metrics_df = pd.DataFrame(results).sort_values(by="f1", ascending=False).reset_index(drop=True)
    best_model_name = metrics_df.loc[0, "model_name"]
    best_model = fitted_models[best_model_name]["model"]

    models_dir.mkdir(parents=True, exist_ok=True)

    # Save full model pipeline
    joblib.dump(best_model, models_dir / "model.pkl")

    # Save just the preprocessor from best pipeline
    best_preprocessor = best_model.named_steps["preprocessor"]
    joblib.dump(best_preprocessor, models_dir / "preprocessor.pkl")

    # Save metrics
    metrics_df.to_csv(models_dir / "metrics_summary.csv", index=False)

    return metrics_df, fitted_models, best_model_name