from src.config import SLA_ROOT_FOLDER, INTERIM_DIR
from src.ingestion.discover_files import discover_prt_files
from src.reconstruction.incremental_table import build_incremental_table
from src.reconstruction.merge_uetr import merge_uetr_rows


def run_pipeline():
    root_folder = SLA_ROOT_FOLDER

    print(f"Scanning root folder: {root_folder}")

    files = discover_prt_files(root_folder)
    print(f"Found {len(files)} .prt files")

    if not files:
        raise FileNotFoundError(f"No .prt files found under {root_folder}")

    incremental_df = build_incremental_table(files, root_folder)
    incremental_output = INTERIM_DIR / "incremental_states.csv"
    incremental_df.to_csv(incremental_output, index=False)

    print(f"Saved incremental table to: {incremental_output}")

    merged_df, unknown_df = merge_uetr_rows(incremental_df)

    merged_output = INTERIM_DIR / "merged_uetr.csv"
    unknown_output = INTERIM_DIR / "unknown_uetr_rows.csv"

    merged_df.to_csv(merged_output, index=False)
    unknown_df.to_csv(unknown_output, index=False)

    print(f"Saved merged UETR table to: {merged_output}")
    print(f"Saved unknown UETR rows to: {unknown_output}")

    print("\nSummary")
    print("-" * 40)
    print(f"Parsed files: {len(files)}")
    print(f"Incremental rows: {len(incremental_df)}")
    print(f"Merged UETR rows: {len(merged_df)}")
    print(f"Unknown UETR rows: {len(unknown_df)}")

    if "Parse_Error" in incremental_df.columns:
        error_count = incremental_df["Parse_Error"].notna().sum()
        print(f"Rows with parse errors: {error_count}")


if __name__ == "__main__":
    run_pipeline()