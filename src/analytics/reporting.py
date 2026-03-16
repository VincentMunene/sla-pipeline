import pandas as pd


def prepare_reporting_fields(df: pd.DataFrame) -> pd.DataFrame:
    temp = df.copy()

    if "start_time" in temp.columns:
        temp["start_time"] = pd.to_datetime(temp["start_time"], errors="coerce")
        temp["date"] = temp["start_time"].dt.date
        temp["year_month"] = temp["start_time"].dt.to_period("M").astype(str)

    # Create one reporting operator field
    candidate_cols = [c for c in ["Creator", "Modifier", "Authorizer", "Authorizer_to_mod"] if c in temp.columns]

    if candidate_cols:
        temp["primary_operator"] = None
        for col in candidate_cols:
            temp["primary_operator"] = temp["primary_operator"].fillna(temp[col])
        temp["primary_operator"] = temp["primary_operator"].fillna("UNKNOWN")
    else:
        temp["primary_operator"] = "UNKNOWN"

    # Loop count proxy
    for col in ["n_mod_auth_events", "n_auth_mod_events", "n_swift_mod_events"]:
        if col not in temp.columns:
            temp[col] = 0

    temp["loop_count"] = (
        temp["n_mod_auth_events"]
        + temp["n_auth_mod_events"]
        + temp["n_swift_mod_events"]
    )

    return temp


def sla_by_operator(df: pd.DataFrame) -> pd.DataFrame:
    temp = prepare_reporting_fields(df)

    out = (
        temp.groupby("primary_operator", dropna=False)
            .agg(
                total_transactions=("primary_operator", "size"),
                breaches=("sla_breached", "sum"),
                avg_processing_minutes=("total_processing_minutes", "mean"),
                modifications=("had_modification", "sum"),
                reworks=("had_rework", "sum"),
                avg_breach_probability=("breach_probability", "mean"),
            )
            .reset_index()
    )

    out["sla_compliance_rate"] = 1 - (out["breaches"] / out["total_transactions"])
    return out.sort_values("total_transactions", ascending=False)


def sla_by_month(df: pd.DataFrame) -> pd.DataFrame:
    temp = prepare_reporting_fields(df)

    out = (
        temp.groupby("year_month", dropna=False)
            .agg(
                total_transactions=("year_month", "size"),
                breaches=("sla_breached", "sum"),
                avg_processing_minutes=("total_processing_minutes", "mean"),
                modifications=("had_modification", "sum"),
                reworks=("had_rework", "sum"),
                avg_breach_probability=("breach_probability", "mean"),
            )
            .reset_index()
    )

    out["sla_compliance_rate"] = 1 - (out["breaches"] / out["total_transactions"])
    return out.sort_values("year_month")


def breach_by_service(df: pd.DataFrame) -> pd.DataFrame:
    out = (
        df.groupby("Service_Name", dropna=False)
          .agg(
              total_transactions=("Service_Name", "size"),
              breaches=("sla_breached", "sum"),
              avg_breach_probability=("breach_probability", "mean"),
          )
          .reset_index()
    )
    out["sla_compliance_rate"] = 1 - (out["breaches"] / out["total_transactions"])
    return out.sort_values("total_transactions", ascending=False)


def breach_by_receiver_bank(df: pd.DataFrame) -> pd.DataFrame:
    out = (
        df.groupby("Receiver_Bank", dropna=False)
          .agg(
              total_transactions=("Receiver_Bank", "size"),
              breaches=("sla_breached", "sum"),
              avg_breach_probability=("breach_probability", "mean"),
          )
          .reset_index()
    )
    out["sla_compliance_rate"] = 1 - (out["breaches"] / out["total_transactions"])
    return out.sort_values("total_transactions", ascending=False)


def loop_count_summary(df: pd.DataFrame) -> pd.DataFrame:
    temp = prepare_reporting_fields(df)

    return (
        temp.groupby("loop_count")
            .size()
            .reset_index(name="transaction_count")
            .sort_values("loop_count")
    )


def daily_summary(df: pd.DataFrame) -> pd.DataFrame:
    temp = prepare_reporting_fields(df)

    out = (
        temp.groupby("date", dropna=False)
            .agg(
                total_transactions=("date", "size"),
                breaches=("sla_breached", "sum"),
                avg_breach_probability=("breach_probability", "mean"),
            )
            .reset_index()
    )

    out["sla_compliance_rate"] = 1 - (out["breaches"] / out["total_transactions"])
    out["date"] = pd.to_datetime(out["date"])
    return out.sort_values("date")