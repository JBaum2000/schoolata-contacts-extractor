from __future__ import annotations
import json, os, shutil, tempfile
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd
from .config import ROOT

# default output file is now XLSX
OUTPUT_DEFAULT = ROOT / "output.xlsx"


# ---------- Excel helpers ----------
def read_input(path: Path) -> pd.DataFrame:
    """Read the input *Excel* with columns id,name (both coerced to str)."""
    df = pd.read_excel(path, dtype={"id": str, "name": str})
    if df.isnull().any().any():
        raise ValueError("Input Excel contains nulls in mandatory columns.")
    return df


def read_output(path: Path) -> pd.DataFrame | None:
    """Read the existing output (if any) as DataFrame."""
    return (
        pd.read_excel(path, dtype={"id": str}, keep_default_na=False)
        if path.exists()
        else None
    )


def atomic_write_excel(df: pd.DataFrame, path: Path) -> None:
    """
    Write DataFrame to XLSX atomically:
    1. write to temp file,
    2. move into place (POSIX-style atomic replace on same filesystem).
    """
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".xlsx", dir=str(path.parent))
    os.close(tmp_fd)
    with pd.ExcelWriter(tmp_path, engine="openpyxl") as xlw:
        df.to_excel(xlw, index=False)
    shutil.move(tmp_path, path)  # atomic on same filesystem


# ---------- JSON-fragment helpers (unchanged) ----------
def append_contact_fragment(tmp_path: Path, profile_json: Dict[str, Any]) -> None:
    with open(tmp_path, "a", encoding="utf-8") as fh:
        json.dump(profile_json, fh, ensure_ascii=False)
        fh.write("\n")
        fh.flush()
        os.fsync(fh.fileno())


def merge_fragments(tmp_path: Path) -> List[Dict[str, Any]]:
    if not tmp_path.exists():
        return []
    with open(tmp_path, encoding="utf-8") as fh:
        return [json.loads(line) for line in fh]


def wipe_fragments(tmp_path: Path) -> None:
    tmp_path.unlink(missing_ok=True)