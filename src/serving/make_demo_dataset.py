import pandas as pd
from pathlib import Path


def make_code_map(values, prefix):
    values = pd.Series(values).dropna().astype(str).str.strip().unique().tolist()
    values = sorted(values)
    return {val: f"{prefix}_{i+1:02d}" for i, val in enumerate(values)}


def anonymize_scored_dataset():
    input_file = Path("data/processed/scored_transactions.csv")
    output_file = Path("data/processed/scored_transactions_demo.csv")

    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")

    df = pd.read_csv(input_file, low_memory=False)

    demo_df = df.copy()

    # -------------------------
    # Preserve a safe reporting timestamp
    # -------------------------
    if "start_time" in demo_df.columns:
        demo_df["start_time"] = pd.to_datetime(demo_df["start_time"], errors="coerce")
        # Round to the nearest hour-equivalent lower bound to reduce sensitivity
        demo_df["start_time"] = demo_df["start_time"].dt.floor("h")

    # -------------------------
    # Drop sensitive columns entirely
    # -------------------------
    sensitive_drop_cols = [
        "UETR",
        "Reference_Number",
        "Filename",
        "Full_Path",
        "Parse_Error",
        "end_time",
    ]

    # Drop raw routing/event timestamps but keep start_time
    sensitive_drop_cols.extend([
        c for c in demo_df.columns
        if "TIME" in c.upper() and c != "start_time"
    ])

    # Drop raw operator identity columns
    sensitive_drop_cols.extend([
        c for c in demo_df.columns
        if c.startswith("Creator")
        or c.startswith("Modifier")
        or c.startswith("Authorizer")
        or c.startswith("Authorizer_to_mod")
    ])

    demo_df = demo_df.drop(
        columns=[c for c in sensitive_drop_cols if c in demo_df.columns],
        errors="ignore"
    )

    # -------------------------
    # Anonymize categorical business fields
    # -------------------------
    if "Sender_Bank" in demo_df.columns:
        sender_map = make_code_map(demo_df["Sender_Bank"], "BANK")
        demo_df["Sender_Bank"] = (
            demo_df["Sender_Bank"]
            .fillna("UNKNOWN")
            .astype(str)
            .str.strip()
            .map(lambda x: sender_map.get(x, "BANK_UNKNOWN"))
        )

    if "Receiver_Bank" in demo_df.columns:
        receiver_map = make_code_map(demo_df["Receiver_Bank"], "BANK")
        demo_df["Receiver_Bank"] = (
            demo_df["Receiver_Bank"]
            .fillna("UNKNOWN")
            .astype(str)
            .str.strip()
            .map(lambda x: receiver_map.get(x, "BANK_UNKNOWN"))
        )

    if "Service_Name" in demo_df.columns:
        service_map = make_code_map(demo_df["Service_Name"], "SERVICE")
        demo_df["Service_Name"] = (
            demo_df["Service_Name"]
            .fillna("UNKNOWN")
            .astype(str)
            .str.strip()
            .map(lambda x: service_map.get(x, "SERVICE_UNKNOWN"))
        )

    if "Message_Type" in demo_df.columns:
        msg_map = make_code_map(demo_df["Message_Type"], "MSG")
        demo_df["Message_Type"] = (
            demo_df["Message_Type"]
            .fillna("UNKNOWN")
            .astype(str)
            .str.strip()
            .map(lambda x: msg_map.get(x, "MSG_UNKNOWN"))
        )

    if "Currency" in demo_df.columns:
        ccy_map = make_code_map(demo_df["Currency"], "CCY")
        demo_df["Currency"] = (
            demo_df["Currency"]
            .fillna("UNKNOWN")
            .astype(str)
            .str.strip()
            .map(lambda x: ccy_map.get(x, "CCY_UNKNOWN"))
        )

    if "primary_operator" in demo_df.columns:
        op_map = make_code_map(demo_df["primary_operator"], "OP")
        demo_df["primary_operator"] = (
            demo_df["primary_operator"]
            .fillna("UNKNOWN")
            .astype(str)
            .str.strip()
            .map(lambda x: op_map.get(x, "OP_UNKNOWN"))
        )

    # -------------------------
    # Round some continuous values
    # -------------------------
    if "breach_probability" in demo_df.columns:
        demo_df["breach_probability"] = pd.to_numeric(
            demo_df["breach_probability"], errors="coerce"
        ).round(4)

    if "total_processing_minutes" in demo_df.columns:
        demo_df["total_processing_minutes"] = pd.to_numeric(
            demo_df["total_processing_minutes"], errors="coerce"
        ).round(2)

    if "Settlement_Amount" in demo_df.columns:
        demo_df["Settlement_Amount"] = pd.to_numeric(
            demo_df["Settlement_Amount"], errors="coerce"
        ).round(2)

    if "log_settlement_amount" in demo_df.columns:
        demo_df["log_settlement_amount"] = pd.to_numeric(
            demo_df["log_settlement_amount"], errors="coerce"
        ).round(4)

    # -------------------------
    # Save
    # -------------------------
    output_file.parent.mkdir(parents=True, exist_ok=True)
    demo_df.to_csv(output_file, index=False)

    print(f"Saved anonymized demo dataset to: {output_file}")
    print(f"Rows: {len(demo_df)}")
    print(f"Columns: {len(demo_df.columns)}")
    print(demo_df.head())
    print("\nColumns:")
    print(demo_df.columns.tolist())


if __name__ == "__main__":
    anonymize_scored_dataset()