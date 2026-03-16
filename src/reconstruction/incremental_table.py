from pathlib import Path
import pandas as pd

from src.ingestion.discover_files import get_folder_category
from src.parsing.regex_parser import extract_advanced_state_data


def build_incremental_table(files: list[Path], root_dir: Path) -> pd.DataFrame:
    rows = []

    for fp in files:
        folder_category = get_folder_category(fp, root_dir)

        try:
            with open(fp, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()

            row = extract_advanced_state_data(
                content=content,
                filename=fp.name,
                folder_category=folder_category
            )
            row["Full_Path"] = str(fp)
            row["Parse_Error"] = None
            rows.append(row)

        except Exception as e:
            rows.append({
                "Filename": fp.name,
                "Folder": folder_category,
                "Full_Path": str(fp),
                "UETR": "UNKNOWN",
                "Parse_Error": str(e)
            })

    return pd.DataFrame(rows)