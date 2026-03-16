import pandas as pd
from pathlib import Path

from src.modeling.train import train_and_compare_models


def run_day3_pipeline():
    input_file = Path("data/processed/modeling_dataset.csv")
    models_dir = Path("data/models")

    print(f"Loading modeling dataset from: {input_file}")
    df = pd.read_csv(input_file, low_memory=False)

    print(f"Dataset shape: {df.shape}")
    print("\nTarget distribution:")
    print(df["sla_breached"].value_counts(dropna=False))

    metrics_df, fitted_models, best_model_name = train_and_compare_models(df, models_dir)

    print("\nModel comparison")
    print("-" * 60)
    print(metrics_df)

    print(f"\nBest model: {best_model_name}")

    print("\nConfusion matrices")
    print("-" * 60)
    for model_name, model_info in fitted_models.items():
        print(f"\n{model_name}")
        print(model_info["confusion_matrix"])

    print("\nSaved outputs:")
    print(models_dir / "model.pkl")
    print(models_dir / "preprocessor.pkl")
    print(models_dir / "metrics_summary.csv")


if __name__ == "__main__":
    run_day3_pipeline()