import pandas as pd


def merge_uetr_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    metadata_cols = [
        "Message_Type",
        "Sender_Bank",
        "Receiver_Bank",
        "Service_Name",
        "Settlement_Amount",
        "Currency",
        "Reference_Number",
    ]

    patterns = [
        "CREATE_AUTH_TIME",
        "MOD_AUTH_TIME",
        "AUTH_MOD_TIME",
        "SWIFT_AUTH_TIME",
        "SWIFT_MOD_TIME",
        "AUTH_SWIFT_TIME",
    ]

    op_map = {
        "CREATE_AUTH_TIME": "Creator",
        "MOD_AUTH_TIME": "Modifier",
        "AUTH_MOD_TIME": "Authorizer_to_mod",
        "AUTH_SWIFT_TIME": "Authorizer",
    }

    def get_all_vals(group: pd.DataFrame, col_prefix: str):
        cols = [c for c in group.columns if c.startswith(col_prefix) and "TIME" in c]
        vals = []

        for c in cols:
            op_col = op_map.get(col_prefix)
            suffix = c.replace(col_prefix, "")
            op_col_name = (op_col + suffix) if op_col else None

            for _, row in group.iterrows():
                time_val = row.get(c)
                if pd.notnull(time_val):
                    try:
                        parsed_time = pd.to_datetime(time_val, dayfirst=True, errors="coerce")
                    except Exception:
                        parsed_time = pd.NaT

                    if pd.notnull(parsed_time):
                        operator = row.get(op_col_name) if op_col_name and op_col_name in group.columns else None
                        vals.append({"time": parsed_time, "op": operator})

        vals = sorted(vals, key=lambda x: x["time"])

        unique_vals = []
        seen = set()
        for v in vals:
            t_str = v["time"].strftime("%d-%m-%Y %H:%M:%S")
            if t_str not in seen:
                unique_vals.append(v)
                seen.add(t_str)

        return unique_vals

    valid_uetr_df = df[df["UETR"] != "UNKNOWN"].copy()
    unknown_df = df[df["UETR"] == "UNKNOWN"].copy()

    merged_data = []

    for uetr, group in valid_uetr_df.groupby("UETR"):
        row = {"UETR": uetr}

        for col in metadata_cols:
            non_nulls = group[col].dropna()
            row[col] = non_nulls.iloc[0] if not non_nulls.empty else "UNKNOWN"

        row["Source_Row_Count"] = len(group)

        for p in patterns:
            vals = get_all_vals(group, p)

            for i, v in enumerate(vals):
                suffix = f"_{i+1}" if i > 0 else ""
                row[f"{p}{suffix}"] = v["time"].strftime("%d-%m-%Y %H:%M:%S")

                if p in op_map:
                    row[f"{op_map[p]}{suffix}"] = v["op"]

        merged_data.append(row)

    merged_df = pd.DataFrame(merged_data)

    priority_cols = [
        "UETR",
        "Message_Type",
        "Sender_Bank",
        "Receiver_Bank",
        "Service_Name",
        "Settlement_Amount",
        "Currency",
        "Reference_Number",
        "Source_Row_Count",
    ]
    other_cols = [c for c in merged_df.columns if c not in priority_cols]
    merged_df = merged_df[priority_cols + sorted(other_cols)]

    return merged_df, unknown_df