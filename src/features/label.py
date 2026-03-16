import pandas as pd


def add_sla_label(df: pd.DataFrame, threshold_minutes: int = 30) -> pd.DataFrame:
    """
    Create the SLA breach label.
    """
    df = df.copy()
    df["sla_breached"] = (df["total_processing_minutes"] > threshold_minutes).astype(int)
    return df