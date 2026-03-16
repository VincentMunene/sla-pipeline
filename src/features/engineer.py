import pandas as pd
import numpy as np


def _count_non_null(df: pd.DataFrame, prefixes: list[str]) -> pd.Series:
    cols = [c for c in df.columns if any(c.startswith(p) for p in prefixes)]
    if not cols:
        return pd.Series(0, index=df.index)
    return df[cols].notna().sum(axis=1)


def _count_touchpoints(df: pd.DataFrame, prefixes: list[str]) -> pd.Series:
    cols = [c for c in df.columns if any(c.startswith(p) for p in prefixes)]
    if not cols:
        return pd.Series(0, index=df.index)
    return df[cols].notna().sum(axis=1)


def _count_unique_operators(row, operator_cols):
    values = []
    for col in operator_cols:
        val = row.get(col)
        if pd.notna(val):
            values.append(str(val).strip().upper())
    return len(set(values))


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # 1. Identify timestamp columns
    time_cols = [c for c in df.columns if "TIME" in c.upper()]

    # 2. Start and end times
    if time_cols:
        df["start_time"] = df[time_cols].min(axis=1)
        df["end_time"] = df[time_cols].max(axis=1)
    else:
        df["start_time"] = pd.NaT
        df["end_time"] = pd.NaT

    # 3. Total processing duration
    df["total_processing_minutes"] = (
        (df["end_time"] - df["start_time"]).dt.total_seconds() / 60
    )

    # 4. Event count features
    df["n_create_auth_events"] = _count_non_null(df, ["CREATE_AUTH_TIME"])
    df["n_mod_auth_events"] = _count_non_null(df, ["MOD_AUTH_TIME"])
    df["n_auth_mod_events"] = _count_non_null(df, ["AUTH_MOD_TIME"])
    df["n_swift_auth_events"] = _count_non_null(df, ["SWIFT_AUTH_TIME"])
    df["n_swift_mod_events"] = _count_non_null(df, ["SWIFT_MOD_TIME"])
    df["n_auth_swift_events"] = _count_non_null(df, ["AUTH_SWIFT_TIME"])
    df["n_available_timestamps"] = _count_non_null(
        df,
        [
            "CREATE_AUTH_TIME",
            "MOD_AUTH_TIME",
            "AUTH_MOD_TIME",
            "SWIFT_AUTH_TIME",
            "SWIFT_MOD_TIME",
            "AUTH_SWIFT_TIME",
        ],
    )

    # 5. Rework flags
    df["had_modification"] = (df["n_mod_auth_events"] > 0).astype(int)
    df["had_auth_to_mod"] = (df["n_auth_mod_events"] > 0).astype(int)
    df["had_swift_modification"] = (df["n_swift_mod_events"] > 0).astype(int)

    df["had_rework"] = (
        (df["had_modification"] == 1)
        | (df["had_auth_to_mod"] == 1)
        | (df["had_swift_modification"] == 1)
    ).astype(int)

    # 6. Operator touchpoint features
    df["n_creator_touchpoints"] = _count_touchpoints(df, ["Creator"])
    df["n_modifier_touchpoints"] = _count_touchpoints(df, ["Modifier"])
    df["n_authorizer_touchpoints"] = _count_touchpoints(df, ["Authorizer"])
    df["n_authorizer_to_mod_touchpoints"] = _count_touchpoints(df, ["Authorizer_to_mod"])

    df["n_total_operator_touchpoints"] = (
        df["n_creator_touchpoints"]
        + df["n_modifier_touchpoints"]
        + df["n_authorizer_touchpoints"]
        + df["n_authorizer_to_mod_touchpoints"]
    )

    operator_cols = [
        c for c in df.columns
        if c.startswith("Creator")
        or c.startswith("Modifier")
        or c.startswith("Authorizer")
        or c.startswith("Authorizer_to_mod")
    ]

    if operator_cols:
        df["n_unique_operators"] = df.apply(
            lambda row: _count_unique_operators(row, operator_cols),
            axis=1
        )
    else:
        df["n_unique_operators"] = 0

    # 7. Calendar / shift features
    df["start_hour"] = df["start_time"].dt.hour
    df["start_dayofweek"] = df["start_time"].dt.dayofweek
    df["start_month"] = df["start_time"].dt.month

    # Shift 1: 7 AM - 3 PM
    # Shift 2: 11 AM - 7 PM
    # Overlap: 11 AM - 3 PM
    df["is_shift_1_hours"] = df["start_hour"].between(7, 14).astype(int)
    df["is_shift_2_hours"] = df["start_hour"].between(11, 18).astype(int)
    df["is_shift_overlap"] = df["start_hour"].between(11, 14).astype(int)
    df["is_shift_hours"] = df["start_hour"].between(7, 18).astype(int)
    df["is_outside_shift_hours"] = (~df["start_hour"].between(7, 18)).astype(int)

    # 8. Amount feature
    if "Settlement_Amount" in df.columns:
        df["log_settlement_amount"] = np.log1p(df["Settlement_Amount"])
    else:
        df["log_settlement_amount"] = np.nan

    return df