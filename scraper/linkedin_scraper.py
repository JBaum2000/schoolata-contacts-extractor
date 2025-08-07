from __future__ import annotations
import json, random, time, pickle
from pathlib import Path
from typing import List, Dict, Any
import requests

try:
    import psutil
except Exception:
    psutil = None

from selenium import webdriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.firefox import GeckoDriverManager
from webdriver_manager.microsoft import EdgeChromiumDriverManager

import time
import os
import tempfile
import uuid
import socket

import subprocess
import shutil

from .config import (
    CHROME_PROFILE_PATH,
    CACHE_DIR,
    LINKEDIN_USERNAME,
    LINKEDIN_PASSWORD,
    HEADLESS,
    BROWSER,
    COOKIE_FILE,
    CHROME_BINARY_PATH, CHROME_USER_DATA_DIR, CHROME_PROFILE_DIRECTORY, CHROME_DEBUG_PORT, CHROME_DEBUG_PORT, FORCE_CLOSE_CHROME,
)
from .linkedin_selectors import Selectors as S
from .prompts import TEMPLATE
from .models import Contact
from openai_api_call import OpenAIIntegration
from .driver_manager import ensure_cft_bundle
from .cookie_bridge import (
    load_cached_cookies,
    save_cached_cookies,
    load_linkedin_cookies_from_chrome,
    inject_cookies,
)

def _pick_free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port

def _devtools_alive(port: int, timeout_sec: float = 0.4) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=timeout_sec):
            pass
        r = requests.get(f"http://127.0.0.1:{port}/json/version", timeout=timeout_sec)
        return r.ok
    except Exception:
        return False

def _profile_in_use_pids(user_data_dir: Path) -> list[int]:
    if not psutil:
        return []
    pids = []
    udd_str = str(user_data_dir).lower()
    for proc in psutil.process_iter(["pid", "name", "cmdline"]):
        try:
            if not proc.info["name"] or "chrome" not in proc.info["name"].lower():
                continue
            cmd = " ".join(proc.info.get("cmdline") or []).lower()
            if "--user-data-dir" in cmd and udd_str in cmd:
                pids.append(proc.pid)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return pids

def _kill_pids(pids: list[int], wait_sec: float = 5.0):
    if not psutil or not pids:
        return
    for pid in pids:
        try:
            p = psutil.Process(pid)
            p.terminate()
        except Exception:
            pass
    # wait a bit, then force-kill any stragglers
    t0 = time.time()
    while time.time() - t0 < wait_sec:
        alive = [pid for pid in pids if psutil.pid_exists(pid)]
        if not alive:
            return
        time.sleep(0.2)
    for pid in [pid for pid in pids if psutil.pid_exists(pid)]:
        try:
            psutil.Process(pid).kill()
        except Exception:
            pass

def _clean_singleton_locks(user_data_dir: Path):
    for name in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
        p = user_data_dir / name
        try:
            if p.exists():
                p.unlink()
        except Exception:
            pass

def _ensure_dirs(user_data_dir: Path, profile_dir: str):
    (user_data_dir / profile_dir).mkdir(parents=True, exist_ok=True)

def _launch_master_chrome_and_get_port() -> int:
    """
    Ensure Chrome is running *with DevTools* for our automation profile and return the port.
    - If CHROME_DEBUG_PORT > 0, try that; else auto-pick a free port.
    - If an existing Chrome holds the profile without DevTools, close it (or ask user).
    """
    user_data_dir = Path(CHROME_USER_DATA_DIR)
    _ensure_dirs(user_data_dir, CHROME_PROFILE_DIRECTORY)

    # if a fixed port is given and already alive, just use it
    if CHROME_DEBUG_PORT > 0 and _devtools_alive(CHROME_DEBUG_PORT):
        return CHROME_DEBUG_PORT

    # if profile is already in use by Chrome *without* DevTools, either close or raise
    pids = _profile_in_use_pids(user_data_dir)
    if pids and not (CHROME_DEBUG_PORT > 0 and _devtools_alive(CHROME_DEBUG_PORT)):
        if FORCE_CLOSE_CHROME:
            _kill_pids(pids)
            time.sleep(0.5)
        else:
            raise RuntimeError(
                f"Chrome is already running with user-data-dir={user_data_dir} but without DevTools.\n"
                f"Close those Chrome windows and rerun, or set FORCE_CLOSE_CHROME=1."
            )

    _clean_singleton_locks(user_data_dir)

    # choose the port
    port = CHROME_DEBUG_PORT if CHROME_DEBUG_PORT > 0 else _pick_free_port()

    # launch Chrome with DevTools on that port
    cmd = [
        CHROME_BINARY_PATH,
        f"--remote-debugging-port={port}",
        f"--user-data-dir={user_data_dir}",
        f"--profile-directory={CHROME_PROFILE_DIRECTORY}",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-extensions",
    ]
    # start detached; don’t capture stdio to avoid Windows handle issues
    subprocess.Popen(cmd, creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)

    # wait for DevTools to come up
    deadline = time.time() + 20
    while time.time() < deadline:
        if _devtools_alive(port):
            return port
        time.sleep(0.25)

    raise RuntimeError(f"Chrome DevTools endpoint did not become ready on port {port}")

def _safe_profile_dir(src: Path | None) -> Path | None:
    """
    If src is the *default* 'User Data' directory, clone it into `.cache`
    and return the clone. Otherwise return src unchanged.
    """
    if src is None:
        return None

    default_dir = (
        Path.home()
        / "AppData"
        / "Local"
        / "Google"
        / "Chrome"
        / "User Data"
    )
    try:
        if src.resolve() == default_dir.resolve():
            clone = CACHE_DIR / "selenium_profile"
            if not clone.exists():
                print(f"⚙️  Cloning default Chrome profile into {clone} …")
                shutil.copytree(src, clone, dirs_exist_ok=True)
            return clone
    except Exception as e:
        print(f"⚠️  Could not verify/clone profile dir: {e}")
    return src


def _devtools_alive(port: int, timeout_sec: float = 0.4) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=timeout_sec):
            pass
        # extra sanity: /json/version
        r = requests.get(f"http://127.0.0.1:{port}/json/version", timeout=timeout_sec)
        return r.ok
    except Exception:
        return False

def _clean_singleton_locks(user_data_dir: Path):
    # Chrome leaves lock files when it crashes; they trigger “profile in use”
    for name in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
        p = user_data_dir / name
        try:
            if p.exists():
                p.unlink()
        except Exception:
            pass

def _ensure_dirs(user_data_dir: Path, profile_dir: str):
    (user_data_dir / profile_dir).mkdir(parents=True, exist_ok=True)

def _launch_master_chrome() -> subprocess.Popen:
    """
    Launch a single Chrome instance with remote debugging + your profile dir.
    If already running (port responds), do nothing.
    """
    if _devtools_alive(CHROME_DEBUG_PORT):
        return None  # already running; we’ll just attach

    udd = Path(CHROME_USER_DATA_DIR)
    _ensure_dirs(udd, CHROME_PROFILE_DIRECTORY)
    _clean_singleton_locks(udd)

    cmd = [
        CHROME_BINARY_PATH,
        f"--remote-debugging-port={CHROME_DEBUG_PORT}",
        f"--user-data-dir={udd}",
        f"--profile-directory={CHROME_PROFILE_DIRECTORY}",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-extensions",
    ]
    # IMPORTANT: Default “User Data” is NOT used here → allowed for DevTools in Chrome 138+

    # Start detached; don’t pipe stdout/stderr (Windows can hang on pipes)
    proc = subprocess.Popen(cmd, creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
    # Wait until DevTools is ready
    deadline = time.time() + 15
    while time.time() < deadline:
        if _devtools_alive(CHROME_DEBUG_PORT):
            return proc
        time.sleep(0.25)
    raise RuntimeError("Chrome DevTools endpoint did not become ready on port " + str(CHROME_DEBUG_PORT))

class LinkedInScraper:
    def __init__(self, headless: bool = HEADLESS):
        self._tmp_user_data_dir: Path | None = None
        self.driver = self._build_driver(headless)
        self.wait = WebDriverWait(self.driver, 20)

    # ---------- driver helpers ----------
    def _build_driver(self, headless: bool):
        from .config import CHROME_PROFILE_PATH  # import inside to avoid cycles

        if BROWSER == "firefox":
            # ... same as before ...
            pass

        elif BROWSER == "edge":
            # ... same as before ...
            pass

        else:  # ---- Chrome ----
            # 1) Ensure a single Chrome is running with our profile + DevTools
            port = _launch_master_chrome_and_get_port()

            # 2) Attach; ChromeDriver will NOT start a new browser
            options = webdriver.ChromeOptions()
            options.add_experimental_option("debuggerAddress", f"127.0.0.1:{port}")

            # 3) Use a matching ChromeDriver (we already fetch same-major)
            _, driver_bin = ensure_cft_bundle()
            service = webdriver.chrome.service.Service(executable_path=str(driver_bin))
            driver = webdriver.Chrome(service=service, options=options)
            return driver
        return driver

    # ---------- auth ----------
    def login(self):
        # Stage 1: cached cookies from our previous successful run
        try:
            cached = load_cached_cookies()
            if inject_cookies(self.driver, cached):
                return
        except Exception:
            pass

        # Stage 2: decrypt cookies from your real Chrome profile (DPAPI)
        try:
            chrome_cookies = load_linkedin_cookies_from_chrome()
            if inject_cookies(self.driver, chrome_cookies):
                save_cached_cookies(self.driver.get_cookies())
                return
        except Exception as e:
            print(f"⚠️  Could not read cookies from Chrome profile: {e}")

        # Stage 3: interactive login once, then cache
        print("🔐 Please complete LinkedIn login in the opened window...")
        self.driver.get("https://www.linkedin.com/login")
        try:
            WebDriverWait(self.driver, 180).until(EC.url_contains("/feed"))
        except Exception:
            # Try submit with env creds if provided (optional convenience)
            if LINKEDIN_USERNAME and LINKEDIN_PASSWORD:
                try:
                    user = WebDriverWait(self.driver, 15).until(
                        EC.presence_of_element_located(("css selector", "input#username, input[name='session_key']"))
                    )
                    pwd = self.driver.find_element("css selector", "input#password, input[name='session_password']")
                    user.clear(); user.send_keys(LINKEDIN_USERNAME)
                    pwd.clear(); pwd.send_keys(LINKEDIN_PASSWORD + "\n")
                    WebDriverWait(self.driver, 180).until(EC.url_contains("/feed"))
                except Exception:
                    pass  # fall through

        if "linkedin.com/feed" not in self.driver.current_url:
            raise RuntimeError("Login not completed. Please sign in within 3 minutes and try again.")

        # Persist cookies for next run
        save_cached_cookies(self.driver.get_cookies())

    def _full_login(self):
        user_box = self.wait.until(EC.element_to_be_clickable((S.SEARCH_BOX[0], 'input[name="session_key"]')))
        user_box.send_keys(LINKEDIN_USERNAME)
        self.driver.find_element("name", "session_password").send_keys(LINKEDIN_PASSWORD + "\n")
        self.wait.until(EC.url_contains("/feed"))

    def _dump_cookies(self):
        with open(COOKIE_FILE, "wb") as fh:
            pickle.dump(self.driver.get_cookies(), fh)

    def _inject_cookies(self):
        self.driver.delete_all_cookies()
        with open(COOKIE_FILE, "rb") as fh:
            for c in pickle.load(fh):
                self.driver.add_cookie(c)

    # ---------- search ----------
    def _human_delay(self, lo=1.5, hi=3.5):
        time.sleep(random.uniform(lo, hi))

    def search_school(self, school_name: str):
        self.driver.get("https://www.linkedin.com")
        box = self.wait.until(EC.element_to_be_clickable(S.SEARCH_BOX))
        box.clear()
        box.send_keys(school_name + "\n")
        self.wait.until(EC.presence_of_element_located(S.PILL_PEOPLE))  # search results loaded
        self._human_delay()

        self.driver.find_element(*S.PILL_PEOPLE).click()
        self._human_delay()

        # open "Current company" dropdown
        self.driver.find_element(*S.PILL_CURRENT_COMPANY).click()
        self.wait.until(EC.presence_of_all_elements_located(S.CURRENT_COMPANY_LIST))
        company_items = self.driver.find_elements(*S.CURRENT_COMPANY_LIST)
        for li in company_items:
            label = li.find_element(*S.CURRENT_COMPANY_LABEL).text.strip().lower()
            if school_name.lower() in label:
                li.find_element("css selector", "input[type='checkbox']").click()
                break
        self.driver.find_element(*S.SHOW_RESULTS).click()
        self._human_delay()

    # ---------- pagination ----------
    def _pagination_state(self) -> tuple[int, int]:
        elem = self.wait.until(EC.presence_of_element_located(S.PAGINATION_STATE))
        txt = elem.text.strip()  # "Page X of Y"
        x, y = [int(p) for p in txt.split() if p.isdigit()]
        return x, y

    def _click_next(self) -> bool:
        try:
            next_btn = self.driver.find_element(*S.PAGINATION_NEXT)
            next_btn.click()
            self._human_delay()
            return True
        except Exception:
            return False

    # ---------- profile harvesting ----------
    def harvest_profiles(self, school_name: str) -> List[Dict[str, Any]]:
        contacts: List[Dict[str, Any]] = []
        page, last = self._pagination_state()
        while True:
            contacts.extend(self._harvest_current_page(school_name))
            page, last = self._pagination_state()
            if page >= last or not self._click_next():
                break
        return contacts

    def _harvest_current_page(self, school_name: str) -> List[Dict[str, Any]]:
        out = []
        links = {a.get_attribute("href") for a in self.driver.find_elements(*S.RESULT_LINKS)}
        for url in links:
            out.append(self._process_profile(url, school_name))
        return out

    def _process_profile(self, url: str, school_name: str) -> Dict[str, Any]:
        # open new tab
        self.driver.execute_script("window.open(arguments[0], '_blank')", url)
        self.driver.switch_to.window(self.driver.window_handles[-1])
        self.wait.until(EC.presence_of_element_located(S.MAIN_TEXT))

        main_text = self.driver.find_element(*S.MAIN_TEXT).text

        # open contact modal
        try:
            self.driver.find_element(*S.CONTACT_INFO_BTN).click()
            self.wait.until(EC.visibility_of_element_located(S.CONTACT_MODAL))
            modal_body = self.driver.find_element(*S.CONTACT_MODAL_BODY)

            # remove upsell
            for upsell in modal_body.find_elements(*S.CONTACT_MODAL_UPSELL):
                self.driver.execute_script("arguments[0].remove()", upsell)

            contact_text = modal_body.text
            self.driver.find_element(*S.CONTACT_MODAL_CLOSE).click()
        except Exception:
            contact_text = ""

        combined_text = main_text + "\n" + contact_text
        prompt = TEMPLATE.format_map({"school_name": school_name, "text": combined_text})

        # ------------- OpenAI -------------
        ai_json_str = OpenAIIntegration.fetch_response(prompt, model="gpt-4o-mini")
        ai_dict = json.loads(ai_json_str)
        contact = Contact.model_validate(ai_dict).model_dump(mode="json")

        self.driver.close()
        self.driver.switch_to.window(self.driver.window_handles[0])
        self._human_delay()
        return contact

    # ---------- teardown ----------
    def close(self):
        try:
            self.driver.quit()
        finally:
            try:
                if self._tmp_user_data_dir and self._tmp_user_data_dir.exists():
                    shutil.rmtree(self._tmp_user_data_dir, ignore_errors=True)
            except Exception:
                pass
