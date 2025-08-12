from __future__ import annotations
import json, os, shutil, tempfile
from pathlib import Path
from typing import List, Dict, Any
import ast
import datetime as _dt
import time as _time

import pandas as pd
from .config import ROOT, TZ_TOLERANCE_HOURS

# default output file is now XLSX
OUTPUT_DEFAULT = ROOT / "output.xlsx"

# --- Geo helpers ---

def get_local_timezone_offset_hours() -> int:
    """Return the local timezone offset from UTC in hours (accounting for DST)."""
    # time.altzone accounts for DST when in effect; use localtime().tm_isdst
    if _time.localtime().tm_isdst and _time.altzone != 0:
        return int(round(-_time.altzone / 3600))
    return int(round(-_time.timezone / 3600))

# A minimal mapping from timezone offset to plausible country codes.
# Note: This is heuristic; a more complete map can be added as needed.
OFFSET_TO_COUNTRIES: dict[int, list[str]] = {
    # UTC-12 to UTC-8
    -12: ["ki"],
    -11: ["as", "nu"],
    -10: ["us"],                     # US-HI
    -9:  ["us", "pf"],              # US-AK
    -8:  ["us", "ca"],              # US-PT, CA-PT
    # UTC-7 to UTC-4
    -7:  ["us", "ca", "mx"],
    -6:  ["us", "ca", "mx"],
    -5:  ["us", "ca"],              # US-ET, CA-ET
    -4:  ["ca", "bm"],
    # UTC-3 to UTC-1
    -3:  ["br", "ar", "uy", "cl"],
    -2:  ["gl"],
    -1:  ["pt"],
    # UTC 0 to UTC+3
    0:   ["gb", "ie", "pt"],
    1:   ["fr", "de", "es", "it", "nl", "be", "ch"],
    2:   ["gr", "ro", "fi", "se", "no", "bg", "ee", "lt", "lv"],
    3:   ["tr", "sa", "iq", "qa", "bh", "kw", "ru", "ua"],
    # UTC+4 to UTC+6
    4:   ["ae", "om", "ru", "az"],
    5:   ["pk", "uz", "tm"],
    6:   ["bd", "kz", "kg"],
    # UTC+7 to UTC+10
    7:   ["th", "vn", "kh", "id"],
    8:   ["cn", "my", "sg", "ph", "au"],
    9:   ["jp", "kr"],
    10:  ["au", "pg"],
    # UTC+11 to UTC+14
    11:  ["sb", "vu"],
    12:  ["nz", "fj"],
    13:  ["to"],
    14:  ["ki"],
}

def choose_country_for_timezone(offset_hours: int, tolerance_hours: int = TZ_TOLERANCE_HOURS) -> str | None:
    """Given a local offset, pick a plausible 2-letter country code within +/- tolerance."""
    candidates: list[str] = []
    for off, countries in OFFSET_TO_COUNTRIES.items():
        if abs(off - offset_hours) <= tolerance_hours:
            candidates.extend(countries)
    if not candidates:
        return None
    # Heuristic preference ordering by commonality
    priority = [
        "us", "ca", "gb", "de", "fr", "es", "it", "nl", "se", "no",
        "au", "jp", "kr", "br", "ar", "mx", "sg", "ae"
    ]
    for code in priority:
        if code in candidates:
            return code
    return candidates[0]

# ---------- Excel helpers ----------
def read_input(path: Path) -> pd.DataFrame:
    """Read the input *Excel* with columns id,name (both coerced to str)."""
    df = pd.read_excel(path, dtype={"id": str, "name": str})
    if df.isnull().any().any():
        raise ValueError("Input Excel contains nulls in mandatory columns.")
    return df


def read_output(path: Path) -> pd.DataFrame | None:
    """Read the existing output (if any) as DataFrame."""
    if not path.exists():
        return None
    
    df = pd.read_excel(path, dtype={"id": str}, keep_default_na=False)

    if "contacts" in df.columns:
        def literal_eval_safe(val):
            if pd.isna(val) or not isinstance(val, str) or not val.startswith('['):
                return []
            try:
                return ast.literal_eval(val)
            except (ValueError, SyntaxError):
                return [] # Return empty list if parsing fails
        df["contacts"] = df["contacts"].apply(literal_eval_safe)

    return df


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