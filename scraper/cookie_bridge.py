from __future__ import annotations
import json, os, time
from pathlib import Path
from typing import List, Dict

import browser_cookie3

from .config import CHROME_PROFILE_PATH, CHROME_PROFILE_NAME, COOKIE_FILE

def _local_state_path(user_data_dir: Path | None) -> Path:
    base = user_data_dir or Path(os.environ["LOCALAPPDATA"]) / "Google" / "Chrome" / "User Data"
    return base / "Local State"

def _candidate_cookie_dbs(user_data_dir: Path | None, profile: str) -> list[Path]:
    base = user_data_dir or Path(os.environ["LOCALAPPDATA"]) / "Google" / "Chrome" / "User Data"
    return [
        base / profile / "Network" / "Cookies",   # new
        base / profile / "Cookies",               # legacy
    ]

def load_linkedin_cookies_from_chrome() -> List[Dict]:
    """Try to decrypt cookies from your real Chrome profile using DPAPI."""
    uds = Path(CHROME_PROFILE_PATH) if CHROME_PROFILE_PATH else None
    key_file = _local_state_path(uds)
    for db in _candidate_cookie_dbs(uds, CHROME_PROFILE_NAME):
        if not db.exists():
            continue
        try:
            # domain_name narrows it, key_file helps on recent Chrome versions
            cj = browser_cookie3.chrome(
                cookie_file=str(db),
                key_file=str(key_file) if key_file.exists() else None,
                domain_name="linkedin.com",
            )
        except TypeError:
            # older browser_cookie3 signature
            cj = browser_cookie3.chrome(cookie_file=str(db))
        cookies: List[Dict] = []
        for c in cj:
            if "linkedin.com" not in c.domain:
                continue
            item = {
                "name": c.name,
                "value": c.value,
                "domain": ".linkedin.com",
                "path": c.path or "/",
            }
            if getattr(c, "expires", None):
                try:
                    item["expiry"] = int(c.expires)
                except Exception:
                    pass
            if getattr(c, "secure", False):
                item["secure"] = True
            cookies.append(item)
        if cookies:
            return cookies
    return []

def load_cached_cookies() -> List[Dict]:
    if COOKIE_FILE.exists():
        try:
            return json.loads(COOKIE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return []

def save_cached_cookies(cookies: List[Dict]) -> None:
    COOKIE_FILE.write_text(json.dumps(cookies, ensure_ascii=False), encoding="utf-8")

def inject_cookies(driver, cookies: List[Dict]) -> bool:
    """Return True if we land authenticated on /feed after injection."""
    if not cookies:
        return False
    driver.get("https://www.linkedin.com/")
    for ck in cookies:
        ck = {k: v for k, v in ck.items() if v is not None}
        try:
            driver.add_cookie(ck)
        except Exception:
            continue
    driver.get("https://www.linkedin.com/feed/")
    time.sleep(1.0)
    return "linkedin.com/feed" in driver.current_url