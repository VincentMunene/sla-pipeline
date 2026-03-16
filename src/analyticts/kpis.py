import pandas as pd


def compute_kpis(df: pd.DataFrame) -> dict:
    total_transactions = len(df)

    actual_breaches = int(df["sla_breached"].sum()) if "sla_breached" in df.columns else None
    predicted_breaches = int((df["prediction"] == 1).sum()) if "prediction" in df.columns else None

    avg_processing_minutes = (
        float(df["total_processing_minutes"].mean())
        if "total_processing_minutes" in df.columns else None
    )

    modification_rate = (
        float(df["had_modification"].mean())
        if "had_modification" in df.columns else None
    )

    rework_rate = (
        float(df["had_rework"].mean())
        if "had_rework" in df.columns else None
    )

    avg_breach_probability = (
        float(df["breach_probability"].mean())
        if "breach_probability" in df.columns else None
    )

    sla_compliance_rate = None
    if "sla_breached" in df.columns and total_transactions > 0:
        sla_compliance_rate = 1 - (actual_breaches / total_transactions)

    return {
        "total_transactions": total_transactions,
        "actual_breaches": actual_breaches,
        "predicted_breaches": predicted_breaches,
        "avg_processing_minutes": avg_processing_minutes,
        "modification_rate": modification_rate,
        "rework_rate": rework_rate,
        "avg_breach_probability": avg_breach_probability,
        "sla_compliance_rate": sla_compliance_rate,
    }