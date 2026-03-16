import pandas as pd


def generate_insights(df: pd.DataFrame) -> list[str]:
    insights = []

    if "breach_probability" in df.columns:
        avg_prob = df["breach_probability"].mean()
        insights.append(f"Average predicted breach probability is {avg_prob:.2%}.")

    if "had_rework" in df.columns:
        rework_rate = df["had_rework"].mean()
        insights.append(f"Rework affects {rework_rate:.2%} of transactions.")

    if "prediction" in df.columns and "start_hour" in df.columns:
        risky_hour = (
            df.groupby("start_hour")["prediction"]
              .mean()
              .sort_values(ascending=False)
              .head(1)
        )
        if not risky_hour.empty:
            insights.append(f"Hour {int(risky_hour.index[0])}:00 has the highest predicted breach rate.")

    if "Service_Name" in df.columns and "sla_breached" in df.columns:
        service_risk = (
            df.groupby("Service_Name")["sla_breached"]
              .mean()
              .sort_values(ascending=False)
              .head(1)
        )
        if not service_risk.empty:
            insights.append(f"Service {service_risk.index[0]} has the highest observed breach rate.")

    return insights