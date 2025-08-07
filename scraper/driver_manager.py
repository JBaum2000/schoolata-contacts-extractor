from __future__ import annotations
import os, platform, requests, tempfile, zipfile, stat, shutil
from pathlib import Path
from typing import Optional, Tuple
import winreg  # safe on Windows; unused on other OS

__all__ = ["ensure_cft_bundle", "detect_chrome_version"]

CF_TESTING_JSON = (
    "https://googlechromelabs.github.io/chrome-for-testing/"
    "known-good-versions-with-downloads.json"
)

PLATFORM_MAP = {
    "Windows": "win64",
    "Linux": "linux64",
    "Darwin": "mac-x64" if platform.machine() == "x86_64" else "mac-arm64",
}

def detect_chrome_version() -> str:
    if platform.system() == "Windows":
        for root in (winreg.HKEY_CURRENT_USER, winreg.HKEY_LOCAL_MACHINE):
            try:
                key = winreg.OpenKey(root, r"SOFTWARE\Google\Chrome\BLBeacon")
                return winreg.QueryValueEx(key, "version")[0]
            except FileNotFoundError:
                continue
        raise RuntimeError("Unable to detect installed Chrome version from registry.")
    else:
        import subprocess, shlex
        for cmd in ("google-chrome --version", "chromium-browser --version", "chrome --version"):
            try:
                out = subprocess.check_output(shlex.split(cmd)).decode().strip()
                return out.split()[-1]
            except Exception:
                pass
        raise RuntimeError("Unable to detect installed Chrome version on POSIX.")

def _get_version_block(chrome_version: str) -> dict:
    data = requests.get(CF_TESTING_JSON, timeout=20).json()
    exact = next((v for v in data["versions"] if v["version"] == chrome_version), None)
    if exact:
        return exact
    major = chrome_version.split(".", 1)[0]
    candidates = [v for v in data["versions"] if v["version"].split(".", 1)[0] == major]
    if candidates:
        return sorted(candidates, key=lambda x: x["version"])[-1]
    # fallback to very latest known-good
    return sorted(data["versions"], key=lambda x: x["version"])[-1]

def _dl(url: str, dest_zip: Path) -> None:
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(dest_zip, "wb") as fh:
            for chunk in r.iter_content(1 << 15):
                fh.write(chunk)

def ensure_cft_bundle(venv_include: Optional[Path] = None) -> Tuple[Path, Path]:
    """
    Download Chrome *and* ChromeDriver from the official CfT bundle matching
    the installed Chrome (exact or same-major). Return (chrome_exe, chromedriver_exe).
    On Windows they will live under: <venv>\Include\cft\<version>\win64\...
    """
    base = venv_include or Path(os.path.dirname(os.__file__)).parent / "Include"
    base.mkdir(parents=True, exist_ok=True)

    target_platform = PLATFORM_MAP[platform.system()]
    chrome_version = detect_chrome_version()
    block = _get_version_block(chrome_version)

    # Figure out urls for this platform
    def find_url(kind: str) -> str:
        for dl in block["downloads"][kind]:
            if dl["platform"] == target_platform:
                return dl["url"]
        raise RuntimeError(f"No {kind} download for platform {target_platform}")

    chrome_url = find_url("chrome")
    driver_url = find_url("chromedriver")

    # Versioned install dir inside the venv Include/
    version_dir = base / "cft" / block["version"] / target_platform
    chrome_dir = version_dir / "chrome"
    driver_dir = version_dir / "driver"
    chrome_dir.mkdir(parents=True, exist_ok=True)
    driver_dir.mkdir(parents=True, exist_ok=True)

    chrome_exe = chrome_dir / ("chrome.exe" if platform.system() == "Windows" else "chrome")
    driver_exe = driver_dir / ("chromedriver.exe" if platform.system() == "Windows" else "chromedriver")

    # --- Download + extract Chrome ---
    if not chrome_exe.exists():
        with tempfile.TemporaryDirectory() as td:
            zip_path = Path(td) / "chrome.zip"
            _dl(chrome_url, zip_path)
            # Extract entire archive, but strip the top-level folder (e.g. chrome-win64/)
            with zipfile.ZipFile(zip_path) as zf:
                for name in zf.namelist():
                    # skip empty entries
                    if not name or name.endswith("/"):
                        # create dirs as needed
                        if name.endswith("/"):
                            rel = "/".join(name.split("/")[1:])  # strip top-level
                            if rel:
                                (chrome_dir / rel).mkdir(parents=True, exist_ok=True)
                        continue
                    # strip the top-level folder
                    rel = "/".join(name.split("/")[1:]) if "/" in name else name
                    target = chrome_dir / rel
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with zf.open(name) as src, open(target, "wb") as dst:
                        shutil.copyfileobj(src, dst)
        try:
            chrome_exe.chmod(chrome_exe.stat().st_mode | stat.S_IEXEC)
        except Exception:
            pass

    # --- Download + extract ChromeDriver ---
    if not driver_exe.exists():
        with tempfile.TemporaryDirectory() as td:
            zip_path = Path(td) / "chromedriver.zip"
            _dl(driver_url, zip_path)
            with zipfile.ZipFile(zip_path) as zf:
                # find the chromedriver binary inside (e.g. chromedriver-win64/chromedriver.exe)
                found = False
                for name in zf.namelist():
                    base_name = os.path.basename(name)
                    if base_name.startswith("chromedriver"):
                        with zf.open(name) as src, open(driver_exe, "wb") as dst:
                            shutil.copyfileobj(src, dst)
                        found = True
                        break
                if not found:
                    raise RuntimeError("Could not locate chromedriver binary in CfT zip.")
        try:
            driver_exe.chmod(driver_exe.stat().st_mode | stat.S_IEXEC)
        except Exception:
            pass

    return chrome_exe, driver_exe