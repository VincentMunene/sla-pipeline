import pandas as pd
import joblib
from pathlib import Path


def run_scoring():
    input_file = Path("data/processed/labeled_features.csv")
    model_file = Path("data/models/model.pkl")
    output_file = Path("data/processed/scored_transactions.csv")

    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")

    if not model_file.exists():
        raise FileNotFoundError(f"Model file not found: {model_file}")

    print(f"Loading labeled features from: {input_file}")
    df = pd.read_csv(input_file, low_memory=False)

    print(f"Loading model from: {model_file}")
    model = joblib.load(model_file)

    # Use the same logic as the trained model expects:
    # remove target + known non-model columns
    cols_to_drop_for_scoring = []

    for col in df.columns:
        if "TIME" in col.upper():
            cols_to_drop_for_scoring.append(col)

    cols_to_drop_for_scoring.extend([
        c for c in df.columns
        if c.startswith("Creator")
        or c.startswith("Modifier")
        or c.startswith("Authorizer")
        or c.startswith("Authorizer_to_mod")
    ])

    cols_to_drop_for_scoring.extend([
        "UETR",
        "Reference_Number",
        "Settlement_Amount",
        "start_time",
        "end_time",
        "total_processing_minutes",
        "is_weekend",
        "is_business_hours",
        "sla_breached",
    ])

    cols_to_drop_for_scoring = [c for c in set(cols_to_drop_for_scoring) if c in df.columns]

    X = df.drop(columns=cols_to_drop_for_scoring, errors="ignore").copy()

    # Align columns to training feature names
    expected_columns = list(model.feature_names_in_)
    X = X.reindex(columns=expected_columns)

    print("Scoring transactions...")
    predictions = model.predict(X)
    probabilities = model.predict_proba(X)[:, 1]

    scored_df = df.copy()
    scored_df["prediction"] = predictions
    scored_df["breach_probability"] = probabilities
    scored_df["interpretation"] = scored_df["prediction"].map({
        1: "Likely SLA breach",
        0: "Not likely to breach SLA"
    })

    if "sla_breached" in scored_df.columns:
        scored_df["prediction_correct"] = (
            scored_df["prediction"] == scored_df["sla_breached"]
        ).astype(int)

    output_file.parent.mkdir(parents=True, exist_ok=True)
    scored_df.to_csv(output_file, index=False)

    print(f"Saved scored transactions to: {output_file}")
    print(f"Rows scored: {len(scored_df)}")
    print(scored_df[["prediction", "breach_probability", "interpretation"]].head())


if __name__ == "__main__":
    run_scoring()