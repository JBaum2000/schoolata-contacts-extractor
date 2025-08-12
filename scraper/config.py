import os
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=ROOT / ".env", override=False)

LINKEDIN_USERNAME = os.getenv("LINKEDIN_USERNAME")
LINKEDIN_PASSWORD = os.getenv("LINKEDIN_PASSWORD")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
BROWSER = os.getenv("BROWSER", "chrome").lower()
CHROME_PROFILE_PATH = os.getenv("CHROME_PROFILE_PATH")
CHROME_PROFILE_NAME = os.getenv("CHROME_PROFILE_NAME", "Default")
CHROME_BINARY_PATH = os.getenv("CHROME_BINARY_PATH", r"C:\Program Files\Google\Chrome\Application\chrome.exe")
CHROME_USER_DATA_DIR = os.getenv("CHROME_USER_DATA_DIR", r"C:\Users\baumj\ChromeAutomation")
CHROME_PROFILE_DIRECTORY = os.getenv("CHROME_PROFILE_DIRECTORY", "OperatorProfile")
CACHE_DIR = ROOT / ".cache"
CACHE_DIR.mkdir(exist_ok=True)
COOKIE_FILE = CACHE_DIR / "linkedin_cookies.json" 
CHROME_DEBUG_PORT = int(os.getenv("CHROME_DEBUG_PORT", "0"))
FORCE_CLOSE_CHROME = os.getenv("FORCE_CLOSE_CHROME", "1") == "1"
PROXY = os.getenv("PROXY")
MAX_PROFILES_PER_DAY = int(os.getenv("MAX_PROFILES_PER_DAY", "100000"))

# --- Data Impulse Proxy Settings ---
USE_DATA_IMPULSE = os.getenv("USE_DATA_IMPULSE", "true").lower() == "true"
DI_USERNAME = os.getenv("DI_USERNAME", "7cc48bb6d6d7def4bd6e")
DI_PASSWORD = os.getenv("DI_PASSWORD", "aa3ca30260efd8ec")
DI_HOST = os.getenv("DI_HOST", "gw.dataimpulse.com")
DI_PORT = int(os.getenv("DI_PORT", "823"))
# Optional geo parameters
GEO_ENFORCE = os.getenv("GEO_ENFORCE", "1") == "1"
DI_COUNTRY = os.getenv("DI_COUNTRY")  # e.g., "us", "gb"; if unset and GEO_ENFORCE=1, we will infer
TZ_TOLERANCE_HOURS = int(os.getenv("TZ_TOLERANCE_HOURS", "1"))
DI_STICKY_SESSION = os.getenv("DI_STICKY_SESSION")  # optional sticky/session id

# Warm-up behavior: "always" or "once"
WARM_UP_MODE = os.getenv("WARM_UP_MODE", "always").lower()  # always | once