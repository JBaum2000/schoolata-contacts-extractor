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