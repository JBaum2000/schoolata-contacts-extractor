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
from selenium.common.exceptions import ElementClickInterceptedException, TimeoutException, StaleElementReferenceException, NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

import re
import time
import os
import tempfile
import uuid
import socket
import traceback
from rapidfuzz import fuzz

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


class NoGoodMatchFound(Exception):
    pass


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

class LinkedInScraper:
    def __init__(self, headless: bool = HEADLESS):
        self._tmp_user_data_dir: Path | None = None
        self.driver = self._build_driver(headless)
        self.wait = WebDriverWait(self.driver, 20)
        self.openai = OpenAIIntegration()

    # ---------- "Humanization" Toolkit ----------
    def _human_delay(self, lo=0.8, hi=2.5):
        """Waits for a random duration to mimic human thinking/reading time."""
        time.sleep(random.uniform(lo, hi))

    def _human_type(self, element: WebElement, text: str):
        """Types text into an element one character at a time with random delays."""
        for char in text:
            element.send_keys(char)
            time.sleep(random.uniform(0.05, 0.15))

    def _human_click(self, element: WebElement):
        """
        Moves to a random location within an element, pauses, and clicks.
        This is a more human-like primary click method.
        """
        try:
            # Calculate a random offset within the element's dimensions
            width, height = element.size['width'], element.size['height']
            x_offset = random.randint(-width // 4, width // 4)
            y_offset = random.randint(-height // 4, height // 4)

            # Move to the element with the random offset, pause, and click
            ActionChains(self.driver).move_to_element(element).move_by_offset(x_offset, y_offset).pause(random.uniform(0.1, 0.4)).click().perform()
        except Exception:
            # If ActionChains fail, fall back to a robust JS click
            self._force_click(element)

    def _human_scroll_down(self, num_scrolls=3, delay_between=0.4):
        """Scrolls down the page in a series of key presses."""
        body = self.driver.find_element(By.TAG_NAME, 'body')
        for _ in range(num_scrolls):
            body.send_keys(Keys.PAGE_DOWN)
            time.sleep(random.uniform(delay_between, delay_between + 0.3))

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
        # If already in feed, we’re logged in – skip prompts.
        self.driver.get("https://www.linkedin.com/feed/")
        self._human_delay(1.5, 2.5)
        if "linkedin.com/feed" in self.driver.current_url:
            print("✅ Already logged in — skipping login.")
            return
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
                    user.clear()
                    self._human_type(user, LINKEDIN_USERNAME)
                    self._human_delay(0.5, 1.0)
                    pwd.clear()
                    self._human_type(pwd, LINKEDIN_PASSWORD)
                    self._human_delay(0.5, 1.0)
                    WebDriverWait(self.driver, 180).until(EC.url_contains("/feed"))
                except Exception:
                    pass  # fall through

        if "linkedin.com/feed" not in self.driver.current_url:
            raise RuntimeError("Login not completed. Please sign in within 3 minutes and try again.")

        # Persist cookies for next run
        save_cached_cookies(self.driver.get_cookies())
    
    def _force_click(self, el):
        """Click element robustly: normal -> scroll -> JS -> ActionChains."""
        try:
            el.click()
            return
        except ElementClickInterceptedException:
            try:
                self.driver.execute_script("arguments[0].scrollIntoView({block:'center',inline:'nearest'});", el)
                time.sleep(random.uniform(0.1, 0.3))
                el.click()
                return
            except Exception:
                pass
        # JS click fallback
        try:
            self.driver.execute_script("arguments[0].click();", el)
            return
        except Exception:
            pass
        # Last resort
        ActionChains(self.driver).move_to_element(el).click().perform()

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
        # 1) Go to LinkedIn and search
        self.driver.get("https://www.linkedin.com")
        box = self.wait.until(EC.element_to_be_clickable(S.SEARCH_BOX))
        box.clear()
        self._human_type(box, school_name)
        self._human_delay(0.5, 1.0)
        box.send_keys(Keys.RETURN)
        people_pill = self.wait.until(EC.presence_of_element_located(S.PILL_PEOPLE))
        self._human_delay()
        self._human_click(people_pill) # Humanized click
        self._human_delay()

        # ---------------- helpers ----------------
        def _best_label_in_items(container, school, min_score=75):
            """
            Finds and returns the <label> element that best matches the school name
            using Levenshtein-based fuzzy string matching, if it meets a min_score.
            """
            def _normalize_text(text: str) -> str:
                """Replaces all whitespace sequences with a single space and strips."""
                if not text:
                    return ""
                # Replace any whitespace character (space, tab, newline, etc.)
                # one or more times with a single space.
                return re.sub(r'\s+', ' ', text).strip()

            items = container.find_elements(By.CSS_SELECTOR, "li.search-reusables__collection-values-item")
            if not items:
                return None

            # Normalize the target school name
            target_school = _normalize_text(school.lower())
            best_match_label = None
            highest_score = -1

            for li in items:
                try:
                    # First, find the overall <label> element, as this is what we need to click.
                    label_element = li.find_element(By.XPATH, ".//label")

                    # --- THIS IS THE FIX ---
                    # Now, find the specific child <span> that contains the visible text
                    # by excluding the one used for screen readers.
                    text_span = label_element.find_element(
                        By.XPATH, ".//span[not(contains(@class, 'visually-hidden'))]"
                    )
                    
                    # Get the text from ONLY that specific span.
                    item_text = _normalize_text(text_span.get_attribute('textContent').lower())

                    # Calculate the similarity score (0-100).
                    score = fuzz.ratio(target_school, item_text)
                    print('item_text', item_text, 'target_school', target_school, 'score', score)
                    
                    # If this item's score is the best we've seen so far, update our best match.
                    if score > highest_score:
                        highest_score = score
                        best_match_label = label_element # Store the whole label for clicking

                except NoSuchElementException:
                    # This list item might not have a label or the expected span, so we skip it.
                    continue
            
            # Only return the label if the best score is good enough.
            if highest_score >= min_score:
                return best_match_label
            
            return None # Otherwise, return None, indicating no good match was found.

        def _open_current_company_container():
            """
            Find the pill, get its aria-controls, and return (pill, container).
            We don't rely on visual hover; we’ll still interact with the elements via JS.
            """
            pill = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//button[@id='searchFilter_currentCompany' or contains(@aria-label,'Current company')]"))
            )
            self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", pill)
            time.sleep(0.05)

            controls_id = (pill.get_attribute("aria-controls") or "").strip()
            container = None

            # Try a few open attempts (hover + click + JS), then locate container by id or outlet
            for _ in range(4):
                try:
                    ActionChains(self.driver).move_to_element(pill).pause(random.uniform(0.1, 0.4)).perform()
                    self._human_click(pill)
                    time.sleep(0.2)
                    if controls_id:
                        container = self.driver.find_element(By.ID, controls_id)
                        if container: return pill, container
                except Exception:
                    pass
                try:
                    outlet = self.driver.find_element(By.ID, "hoverable-outlet-current-company-filter-value")
                    container = outlet.find_element(
                        By.XPATH,
                        ".//div[contains(@class,'artdeco-hoverable-content') and @role='tooltip']"
                    )
                    if container: return pill, container
                except Exception:
                    pass
            return pill, None

        def _apply_selection_in_container(container, school):
            """Click label (via JS if necessary) and then click 'Show results' inside same container."""
            # Pick a label using our fuzzy match logic
            label = _best_label_in_items(container, school)
            if not label:
                return False

            # Select checkbox by clicking the <label> (JS click to bypass overlay issues)
            try:
                self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", label)
                time.sleep(0.05)
                try:
                    label.click()
                except Exception:
                    self.driver.execute_script("arguments[0].click();", label)
            except Exception:
                return False

            # Click Show results in THIS container
            try:
                show_btn = WebDriverWait(container, 8).until(
                    EC.presence_of_element_located((
                        By.XPATH,
                        ".//button[@aria-label='Apply current filter to show results' or .//span[normalize-space()='Show results']]"
                    ))
                )
                self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", show_btn)
                time.sleep(0.05)
                # Use JS to avoid intercept
                self.driver.execute_script("arguments[0].click();", show_btn)

                # Wait for results to refresh: URL change or list staleness
                old_url = self.driver.current_url
                try:
                    old_list = self.driver.find_element(By.CSS_SELECTOR, "ul.reusable-search__entity-result-list, ul.search-results__list")
                except Exception:
                    old_list = None

                WebDriverWait(self.driver, 10).until(
                    lambda d: d.current_url != old_url or (old_list and EC.staleness_of(old_list)(d))
                )
                self._human_delay()
                return True
            except Exception:
                return False

        # ---------------- Strategy A: try the pill container ----------------
        try:
            pill, container = _open_current_company_container()
            if container and _apply_selection_in_container(container, school_name):
                return
        except Exception:
            pass

        # ---------------- Strategy B: All filters fallback ----------------
        try:
            all_btn = WebDriverWait(self.driver, 8).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(@aria-label,'All filters')]"))
            )
            self._force_click(all_btn)

            drawer = WebDriverWait(self.driver, 10).until(
                EC.visibility_of_element_located((By.XPATH, "//div[@role='dialog'] | //div[contains(@class,'filters-bar-expanded')]"))
            )

            # MODIFIED BLOCK: Use the fuzzy matching helper for consistency and accuracy.
            label_to_click = _best_label_in_items(drawer, school_name)

            if label_to_click:
                self._force_click(label_to_click)
            else:
                # If no label meets the minimum score, this strategy fails.
                raise NoGoodMatchFound(f"No company match found for '{school_name}' in 'All filters' with sufficient similarity.")

            show_btn = WebDriverWait(drawer, 10).until(EC.element_to_be_clickable((
                By.XPATH,
                ".//button[@aria-label='Apply current filter to show results' or .//span[normalize-space()='Show results']]"
            )))
            self._force_click(show_btn)

            try:
                WebDriverWait(self.driver, 10).until(EC.invisibility_of_element(drawer))
            except Exception:
                pass

            self._human_delay()
            return
        except Exception as e:
            # If the exception was our specific one, re-raise it to be caught by main.py
            if isinstance(e, NoGoodMatchFound):
                raise e
            
            try:
                self._debug_dump("current_company_fail")
            except Exception:
                pass
            
            # If any other error occurs or both strategies fail, raise our specific exception.
            raise NoGoodMatchFound(f"Could not select a 'Current company' for '{school_name}' with sufficient similarity.")

            
    def _open_current_company_dropdown(self) -> bool:
        # Find the pill by its stable ID first; fall back to aria-label
        try:
            btn = WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located((By.ID, "searchFilter_currentCompany"))
            )
        except TimeoutException:
            try:
                btn = WebDriverWait(self.driver, 8).until(
                    EC.presence_of_element_located(
                        (By.XPATH, "//button[contains(@id,'searchFilter_currentCompany') or contains(@aria-label,'Current company')]")
                    )
                )
            except TimeoutException:
                return False

        controls_id = (btn.get_attribute("aria-controls") or "").strip()

        def is_open(_drv):
            try:
                if btn.get_attribute("aria-expanded") == "true":
                    return True
                if controls_id:
                    el = _drv.find_element(By.ID, controls_id)
                    return el.is_displayed()
            except Exception:
                pass
            return False

        for _ in range(6):
            # close any other open popover
            try:
                ActionChains(self.driver).send_keys(Keys.ESCAPE).perform()
                time.sleep(0.05)
            except Exception:
                pass

            self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            time.sleep(0.05)

            # try: normal click → Actions → JS → elementFromPoint
            try:
                btn.click()
            except Exception:
                try:
                    ActionChains(self.driver).move_to_element(btn).pause(0.05).click().perform()
                except Exception:
                    try:
                        self.driver.execute_script("arguments[0].click();", btn)
                    except Exception:
                        try:
                            x, y = self.driver.execute_script(
                                "const r=arguments[0].getBoundingClientRect();"
                                "return [Math.floor(r.left+r.width/2), Math.floor(r.top+r.height/2)];",
                                btn
                            )
                            self.driver.execute_script(
                                "document.elementFromPoint(arguments[0],arguments[1]).click()", x, y
                            )
                        except Exception:
                            pass

            try:
                WebDriverWait(self.driver, 3).until(lambda d: is_open(d))
                return True
            except TimeoutException:
                continue

        # optional: capture HTML/screenshot if you have _debug_dump
        try:
            self._debug_dump("current_company_open_fail")
        except Exception:
            pass
        return False

    def _get_page_numbers(self) -> tuple[int, int]:
        """
        Parses 'Page X of Y' from the page-state element, handling lazy-loading.
        """
        try:
            # 1. Scroll to the absolute bottom of the page. This is essential
            # to trigger the JavaScript that renders the pagination element.
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            # 2. Define a specific locator to avoid finding hidden duplicates.
            locator = (By.CLASS_NAME, "artdeco-pagination")

            # 3. Explicitly wait for the element to become VISIBLE. This is the
            # crucial step that solves the race condition. The script will poll
            # the DOM for up to 12 seconds until the element appears.
            state_el = WebDriverWait(self.driver, 12).until(
                EC.visibility_of_element_located(locator)
            )

            # 4. Once visible, get the text and parse it.
            txt = state_el.text.strip()
            m = re.search(r"Page\s+(\d+)\s+of\s+(\d+)", txt)
            
            if not m:
                # This error is a safeguard in case the element is visible but empty.
                raise RuntimeError(f"Found pagination element but could not parse text: {txt!r}")
                
            return int(m.group(1)), int(m.group(2))

        except TimeoutException:
            # This block now correctly catches cases where, even after scrolling and
            # waiting, no pagination element ever becomes visible (e.g., on a page
            # with only one page of results).
            print("🟡 No visible pagination element found after scroll and wait, assuming 1 page.")
            return 1, 1

    def _current_results_marker(self) -> str:
        """Fingerprint current results (first card urn or first profile href)."""
        try:
            first_card = self.driver.find_element(By.XPATH, "(//div[@data-chameleon-result-urn])[1]")
            urn = first_card.get_attribute("data-chameleon-result-urn") or ""
            if urn:
                return urn
        except Exception:
            pass
        try:
            first_link = self.driver.find_element(By.XPATH, "(//a[contains(@href,'/in/')])[1]")
            return first_link.get_attribute("href") or ""
        except Exception:
            return ""

    def _first_result_href(self) -> str | None:
        """Get the href of the first visible profile name link."""
        try:
            el = self.driver.find_element(
                By.XPATH,
                "(//div[@data-chameleon-result-urn and contains(@data-view-name,'search-entity-result')]"
                "//div[contains(@class,'mb1')]//a[contains(@href,'/in/')])[1]"
            )
            return (el.get_attribute("href") or "").split("?")[0]
        except Exception:
            return None

    def _click_next_page(self, current_page: int) -> bool:
        """
        Click Next and wait until either page number increments or the first result changes.
        """
        old_url = self.driver.current_url
        old_mark = self._current_results_marker()

        try:
            next_btn = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((
                    By.XPATH,
                    "//button[contains(@class,'artdeco-pagination__button--next') and not(@disabled)]"
                    " | //button[@aria-label='Next' and not(@disabled)]"
                    " | //button[.//span[normalize-space()='Next'] and not(@disabled)]"
                ))
            )
        except TimeoutException:
            print("ℹ️ No Next button available (likely last page).")
            return False

        self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", next_btn)
        time.sleep(0.05)
        try:
            next_btn.click()
        except Exception:
            try:
                ActionChains(self.driver).move_to_element(next_btn).pause(0.05).click().perform()
            except Exception:
                self.driver.execute_script("arguments[0].click();", next_btn)

        def page_or_results_changed(_):
            try:
                state = self.driver.find_element(*S.PAGINATION_STATE).text
                m = re.search(r"Page\s+(\d+)\s+of\s+(\d+)", state or "")
                if m and int(m.group(1)) > current_page:
                    return True
            except Exception:
                pass
            now = self._current_results_marker()
            return (old_mark and now and now != old_mark) or (self.driver.current_url != old_url)

        try:
            WebDriverWait(self.driver, 18).until(page_or_results_changed)
            self._human_delay(0.8, 1.6)
            return True
        except TimeoutException:
            print("⚠️ Next click didn’t change results; staying on this page.")
            return False

    def harvest_profiles(self, school_name: str) -> List[Dict[str, Any]]:
        """Harvest all pages (crash-safe; each profile is persisted immediately)."""
        contacts: List[Dict[str, Any]] = []
        cur, last = self._get_page_numbers()
        while True:
            contacts.extend(self._harvest_current_page(school_name))
            try:
                cur, last = self._get_page_numbers()
            except Exception:
                # If pagination element disappeared but URL changed, just try next once
                cur += 1
            if cur >= last:
                break
            if not self._click_next_page(cur):
                print("ℹ️ Could not advance to next page (maybe last page?).")
                break
        return contacts


    def _harvest_current_page(self, school_name: str) -> List[Dict[str, Any]]:
        """
        Open each profile link (from the name row only), extract, close tab, repeat.
        We collect HREFs first to avoid stale <li> elements as the DOM reflows.
        """
        print("Harvesting current page")
        contacts: List[Dict[str, Any]] = []
        hrefs = self._collect_profile_links()

        for idx, href in enumerate(hrefs, 1):
            try:
                print(f"  → [{idx}/{len(hrefs)}] Opening {href}")
                # Open in a new tab + switch
                self.driver.execute_script("window.open(arguments[0], '_blank')", href)
                self.driver.switch_to.window(self.driver.window_handles[-1])

                # Wait for profile main area
                WebDriverWait(self.driver, 20).until(EC.presence_of_element_located(S.MAIN_TEXT))

                # Extract on this tab
                contact = self._extract_profile_current_tab(school_name, href)
                contacts.append(contact)

            except Exception as e:
                print(f"    ⚠️  Skipping profile due to error:\n{traceback.format_exc()}")
            finally:
                # Always close the profile tab and go back to results
                try:
                    if len(self.driver.window_handles) > 1:
                        self.driver.close()
                        self.driver.switch_to.window(self.driver.window_handles[0])
                        time.sleep(0.2)
                except Exception:
                    pass

        print(f"✅ Harvested {len(contacts)} contacts from this page")
        return contacts
    
    def _extract_profile_current_tab(self, school_name: str, href: str) -> Dict[str, Any]:
        """Assumes we're already on a profile tab. Extracts text, opens contact modal, calls OpenAI, persists."""
        # 1) Main profile text
        main_text = ""
        try:
            main_text = self.driver.find_element(*S.MAIN_TEXT).text
        except Exception as e:
            print(f"    ⚠️  Could not read main profile text: {repr(e)}")

        # 2) Contact info modal (wait until the modal body actually contains text)
        contact_text = ""
        try:
            btn = WebDriverWait(self.driver, 15).until(EC.element_to_be_clickable(S.CONTACT_INFO_BTN))
            self._force_click(btn)
            time.sleep(1)

            modal = WebDriverWait(self.driver, 20).until(EC.visibility_of_element_located(S.CONTACT_MODAL))
            body = WebDriverWait(modal, 20).until(EC.presence_of_element_located(S.CONTACT_MODAL_BODY))

            # Let content populate (poll for some text)
            t0 = time.time()
            while time.time() - t0 < 8:
                txt = (body.text or "").strip()
                if len(txt) > 10:
                    break
                time.sleep(0.25)

            # Strip any upsell
            try:
                for upsell in modal.find_elements(*S.CONTACT_MODAL_UPSELL):
                    self.driver.execute_script("arguments[0].remove()", upsell)
            except Exception:
                pass

            contact_text = body.text
            # Close modal
            try:
                self.driver.find_element(*S.CONTACT_MODAL_CLOSE).click()
            except Exception:
                pass

        except Exception as e:
            print(f"    (no or slow contact modal) {repr(e)}")

        # 3) Send to OpenAI
        combined_text = (main_text or "") + "\n" + (contact_text or "")
        prompt = TEMPLATE.format_map({"school_name": school_name, "text": combined_text})

        try:
            ai_json_str = self.openai.fetch_response(prompt, model="gpt-4o-mini")
            ai_dict = json.loads(ai_json_str)
            contact = Contact.model_validate(ai_dict).model_dump(mode="json")
        except Exception as e:
            print(f"    ⚠️  OpenAI parse failed: {repr(e)}")
            contact = {
                "name": None,
                "title": None,
                "department": None,
                "email": None,
                "phone": None,
                "linkedin_url": href,
                "bio": None,
            }

        # 4) Always persist immediately to avoid data loss
        try:
            # Make sure the URL is present
            contact.setdefault("linkedin_url", href)
            self._persist_contact(school_name, contact)
            print(f"    💾 Saved: {json.dumps(contact, ensure_ascii=False)}")
        except Exception as e:
            print(f"    ⚠️  Failed to persist: {repr(e)}")

        return contact


    def _process_profile(self, url: str, school_name: str) -> Dict[str, Any]:
        """
        Open profile in a new tab, scrape main text + contact modal, call OpenAI,
        persist result to disk, close tab, and return to results.
        """
        parent = self.driver.current_window_handle
        self.driver.execute_script("window.open(arguments[0], '_blank')", url)
        self.driver.switch_to.window(self.driver.window_handles[-1])

        try:
            WebDriverWait(self.driver, 20).until(EC.presence_of_element_located(S.MAIN_TEXT))
            main_text = self.driver.find_element(*S.MAIN_TEXT).text

            # Contact modal with robust waits
            contact_text = ""
            try:
                btn = WebDriverWait(self.driver, 8).until(EC.element_to_be_clickable(S.CONTACT_INFO_BTN))
                self._force_click(btn)

                modal = WebDriverWait(self.driver, 15).until(EC.visibility_of_element_located(S.CONTACT_MODAL))
                body = WebDriverWait(modal, 15).until(EC.presence_of_element_located(S.CONTACT_MODAL_BODY))

                # allow content to populate
                t0 = time.time()
                while time.time() - t0 < 5 and len((body.text or "").strip()) < 20:
                    time.sleep(0.2)

                # Strip upsell if present
                try:
                    for upsell in modal.find_elements(*S.CONTACT_MODAL_UPSELL):
                        self.driver.execute_script("arguments[0].remove()", upsell)
                except Exception:
                    pass

                contact_text = body.text
            except Exception:
                contact_text = ""

            combined_text = (main_text or "") + "\n" + (contact_text or "")
            prompt = TEMPLATE.format_map({"school_name": school_name, "text": combined_text})

            # Call OpenAI and validate output
            ai_json_str = self.openai.fetch_response(prompt, model="gpt-4o-mini")
            ai_dict = json.loads(ai_json_str)
            contact = Contact.model_validate(ai_dict).model_dump(mode="json")
            contact.setdefault("linkedin_url", url)

            print(f"🧾 Extracted: {json.dumps(contact, ensure_ascii=False)}")
            self._persist_contact(school_name, contact)
            return contact

        finally:
            # Best-effort close modal
            try:
                for btn in self.driver.find_elements(*S.CONTACT_MODAL_CLOSE):
                    try:
                        self._force_click(btn)
                        time.sleep(0.2)
                        break
                    except Exception:
                        pass
            except Exception:
                pass

            # Close the profile tab & return to the results tab
            try:
                self.driver.close()
            finally:
                self.driver.switch_to.window(parent)
                self._human_delay()

    def _persist_contact(self, school_name: str, contact: Dict[str, Any]) -> None:
        """Write each contact to disk right away to avoid data loss."""
        try:
            safe_school = re.sub(r'[^a-z0-9]+', '-', school_name.lower()).strip('-')
            outdir = CACHE_DIR / "contacts_spool" / safe_school
            outdir.mkdir(parents=True, exist_ok=True)

            key = contact.get("linkedin_url") or contact.get("name") or str(uuid.uuid4())
            key = re.sub(r'[^a-z0-9]+', '-', key.lower()).strip('-')[:80]

            path = outdir / f"{int(time.time()*1000)}-{key}.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(contact, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"⚠️  Failed to persist contact: {e}")

    
    def _school_slug(self, name: str) -> str:
        return "".join(c.lower() if c.isalnum() else "-" for c in name).strip("-")

    def _persist_contact(self, school_name: str, contact: dict) -> None:
        """
        Append one JSON object per line to a per-school file:
        .cache/runs/<school-slug>.jsonl
        """
        try:
            run_dir = (CACHE_DIR / "runs")
            run_dir.mkdir(parents=True, exist_ok=True)
            fpath = run_dir / f"{self._school_slug(school_name)}.jsonl"
            with open(fpath, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(contact, ensure_ascii=False) + "\n")
        except Exception as e:
            print(f"⚠️ Failed to persist contact: {e}")

    def _collect_profile_links(self) -> List[str]:
        print("Collecting profile links")
        """
        Collect only the main profile link from each result card.
        We rely on the stable 'mb1' block (the profile name row) and exclude
        anything inside the 'entity-result__insights' (mutual connections) area.
        """
        hrefs: List[str] = []
        seen: set[str] = set()

        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)

        # Make sure the results list exists
        WebDriverWait(self.driver, 12).until(
            EC.presence_of_element_located((By.XPATH, "//ul[@role='list']"))
        )

        anchors = self.driver.find_elements(
            By.XPATH,
            "//div[@data-chameleon-result-urn and contains(@data-view-name,'search-entity-result')]"
            "//div[contains(@class,'mb1')]"
            "//a[contains(@href,'/in/') and not(ancestor::div[contains(@class,'entity-result__insights')])]"
        )

        # Fallback if LinkedIn shuffled the structure
        if not anchors:
            anchors = self.driver.find_elements(
                By.XPATH,
                "//ul[@role='list']"
                "//div[contains(@class,'mb1')]"
                "//a[contains(@href,'/in/') and not(ancestor::div[contains(@class,'entity-result__insights')])]"
            )

        for a in anchors:
            href = (a.get_attribute("href") or "").strip()
            if "/in/" not in href:
                continue
            canon = href.split("?")[0]
            if canon in seen:
                continue
            seen.add(canon)
            hrefs.append(href)

        print(f"🔎 Found {len(hrefs)} profile links on this page")
        self._human_delay(0.5, 1.1)
        return hrefs
    
    def _current_results_marker(self) -> str:
        """
        Fingerprint the current page of results to detect page change after clicking 'Next'.
        Prefer the first result's data-chameleon-result-urn; fallback to first profile link.
        """
        try:
            first_card = self.driver.find_element(
                By.XPATH, "(//div[@data-chameleon-result-urn])[1]"
            )
            urn = first_card.get_attribute("data-chameleon-result-urn") or ""
            if urn:
                return urn
        except Exception:
            pass

        try:
            first_link = self.driver.find_element(
                By.XPATH, "(//a[contains(@href,'/in/')])[1]"
            )
            return first_link.get_attribute("href") or ""
        except Exception:
            return ""

    def _debug_dump(self, tag: str):
        try:
            Path("debug").mkdir(exist_ok=True)
            ts = str(int(time.time()))
            self.driver.save_screenshot(f"debug/{ts}_{tag}.png")
            Path(f"debug/{ts}_{tag}.html").write_text(self.driver.page_source, encoding="utf-8")
        except Exception:
            pass

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
