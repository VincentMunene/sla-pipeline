import pandas as pd
from statsmodels.tsa.holtwinters import ExponentialSmoothing


def daily_transaction_counts(df: pd.DataFrame) -> pd.DataFrame:
    temp = df.copy()
    temp["start_time"] = pd.to_datetime(temp["start_time"], errors="coerce")
    temp["date"] = temp["start_time"].dt.date

    out = temp.groupby("date").size().reset_index(name="transaction_count")
    out["date"] = pd.to_datetime(out["date"])
    return out.sort_values("date")


def forecast_transaction_volume(daily_df: pd.DataFrame, periods: int = 7) -> pd.DataFrame:
    series = daily_df.set_index("date")["transaction_count"].asfreq("D").fillna(0)

    model = ExponentialSmoothing(
        series,
        trend="add",
        seasonal=None,
        initialization_method="estimated"
    ).fit()

    forecast = model.forecast(periods)
    result = forecast.reset_index()
    result.columns = ["date", "forecast_transaction_count"]
    return result