import pandas as pd

from src.config import INTERIM_DIR, PROCESSED_DIR
from src.features.clean import clean_merged_uetr_df
from src.features.engineer import engineer_features
from src.features.label import add_sla_label
from src.features.select_model_features import build_modeling_dataset

def run_day2_pipeline():
    input_file = INTERIM_DIR / "merged_uetr.csv"

    print(f"Loading merged data from: {input_file}")
    df = pd.read_csv(input_file)

    print(f"Initial shape: {df.shape}")

    # -------------------------
    # Clean
    # -------------------------
    cleaned_df = clean_merged_uetr_df(df)
    cleaned_output = PROCESSED_DIR / "cleaned_merged_uetr.csv"
    cleaned_df.to_csv(cleaned_output, index=False)
    print(f"Saved cleaned dataset to: {cleaned_output}")

    # -------------------------
    # Engineer features
    # -------------------------
    features_df = engineer_features(cleaned_df)
    features_output = PROCESSED_DIR / "feature_engineered_uetr.csv"
    features_df.to_csv(features_output, index=False)
    print(f"Saved feature-engineered dataset to: {features_output}")

    # -------------------------
    # Label
    # -------------------------
    labeled_df = add_sla_label(features_df, threshold_minutes=30)
    labeled_output = PROCESSED_DIR / "labeled_features.csv"
    labeled_df.to_csv(labeled_output, index=False)
    print(f"Saved labeled dataset to: {labeled_output}")

    # -------------------------
    # Build modeling dataset
    # -------------------------
    modeling_df = build_modeling_dataset(labeled_df)
    modeling_output = PROCESSED_DIR / "modeling_dataset.csv"
    modeling_df.to_csv(modeling_output, index=False)
    print(f"Saved modeling dataset to: {modeling_output}")

    # -------------------------
    # Summary checks
    # -------------------------
    print("\nSummary")
    print("-" * 40)
    print(f"Rows: {len(labeled_df)}")
    print(f"Columns: {len(labeled_df.columns)}")
    print(f"Rows with valid start_time: {labeled_df['start_time'].notna().sum()}")
    print(f"Rows with valid end_time: {labeled_df['end_time'].notna().sum()}")
    print(f"Rows with valid duration: {labeled_df['total_processing_minutes'].notna().sum()}")
    print(f"Rows with negative duration: {(labeled_df['total_processing_minutes'] < 0).sum()}")
    print("\nSLA breach distribution:")
    print(labeled_df["sla_breached"].value_counts(dropna=False))

    print("\nDuration summary:")
    print(labeled_df["total_processing_minutes"].describe())


if __name__ == "__main__":
    run_day2_pipeline()