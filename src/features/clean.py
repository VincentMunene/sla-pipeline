import pandas as pd
import numpy as np


def clean_merged_uetr_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the merged UETR dataset:
    - standardize missing placeholders
    - parse timestamp columns
    - convert numeric columns
    """

    df = df.copy()

    # Standardize common placeholder values
    placeholders = {"UNKNOWN", "N/A", "NA", "NONE", "", "NULL", "nan"}
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].apply(
                lambda x: np.nan if isinstance(x, str) and x.strip().upper() in placeholders else x
            )

    # Identify time columns
    time_cols = [c for c in df.columns if "TIME" in c.upper()]

    # Parse time columns using day-first format
    for col in time_cols:
        df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce")

    # Numeric conversion
    numeric_cols = [c for c in ["Settlement_Amount", "Source_Row_Count"] if c in df.columns]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

        
    # Handle categorical collumns with blanks        
    categorical_cols = ["Message_Type", "Sender_Bank", "Receiver_Bank", "Service_Name", "Currency"]

    for col in categorical_cols:
        if col in df.columns:
            df[col] = df[col].fillna("UNKNOWN").astype(str).str.strip()
            df[col] = df[col].replace({"": "UNKNOWN", "UNK": "UNKNOWN"})

    return df