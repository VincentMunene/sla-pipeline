from pathlib import Path


def discover_prt_files(root_dir: Path) -> list[Path]:
    """
    Recursively find all .prt files under the root folder.
    """
    return sorted(root_dir.rglob("*.prt"))


def get_folder_category(file_path: Path, root_dir: Path) -> str:
    """
    Returns the first subfolder name under the root folder.
    Example:
    root = C:\\SLA\\SLA
    file = C:\\SLA\\SLA\\AUTHMOD\\file1.prt
    returns AUTHMOD
    """
    try:
        relative_parts = file_path.relative_to(root_dir).parts
        if len(relative_parts) >= 2:
            return relative_parts[0].upper()
        return file_path.parent.name.upper()
    except Exception:
        return file_path.parent.name.upper()