import pandas as pd


def build_modeling_dataset(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    cols_to_drop = []

    # Drop raw timestamp columns
    cols_to_drop.extend([c for c in df.columns if "TIME" in c.upper()])

    # Drop raw operator identity columns
    cols_to_drop.extend([
        c for c in df.columns
        if c.startswith("Creator")
        or c.startswith("Modifier")
        or c.startswith("Authorizer")
        or c.startswith("Authorizer_to_mod")
    ])

    # Drop identifiers / raw fields / leakage-prone fields / unused fields
    cols_to_drop.extend([
        "UETR",
        "Reference_Number",
        "Settlement_Amount",
        "start_time",
        "end_time",
        "total_processing_minutes",
        "is_weekend",
        "is_business_hours",
    ])

    cols_to_drop = list(set([c for c in cols_to_drop if c in df.columns]))

    modeling_df = df.drop(columns=cols_to_drop, errors="ignore")

    return modeling_df