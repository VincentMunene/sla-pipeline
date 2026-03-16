from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
INTERIM_DIR = DATA_DIR / "interim"
PROCESSED_DIR = DATA_DIR / "processed"

for folder in [RAW_DIR, INTERIM_DIR, PROCESSED_DIR]:
    folder.mkdir(parents=True, exist_ok=True)

# Change this if needed
SLA_ROOT_FOLDER = Path(r"C:\Users\Spectre\PROJECTS\sla_pipeline\SLA\SLA")

EXPECTED_STATE_FOLDERS = {
    "AUTHMOD",
    "AUTHSWIFT",
    "CREATAUTH",
    "MODAUTH",
    "SWIFTMOD",
}