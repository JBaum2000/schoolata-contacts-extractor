from __future__ import annotations
import json, random, time, pickle
from pathlib import Path
from typing import List, Dict, Any, Generator
import requests

try:
    import psutil
except Exception:
    psutil = None

import undetected_chromedriver as uc
try:
    from seleniumwire import webdriver as wire_webdriver
    SELENIUM_WIRE_AVAILABLE = True
except ImportError:
    SELENIUM_WIRE_AVAILABLE = False

# For Chrome service with output redirection
from selenium.webdriver.chrome.service import Service as ChromeService
import subprocess
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import ElementClickInterceptedException, TimeoutException, StaleElementReferenceException, NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

import re
import time
import os
import sys
import tempfile
import uuid
import socket
import traceback
from rapidfuzz import fuzz
from fake_useragent import UserAgent

import subprocess
import shutil
import platform, requests, tempfile, zipfile, stat, shutil
from pathlib import Path
from typing import Optional, Tuple
import winreg  # safe on Windows; unused on other OS

__all__ = ["ensure_cft_bundle", "detect_chrome_version", "get_chrome_main_version"]

def get_real_chrome_versions() -> list:
    """
    Fetches real Chrome version numbers from Google's Version History API.
    Returns a list of recent stable version strings.
    """
    try:
        response = requests.get(
            "https://versionhistory.googleapis.com/v1/chrome/platforms/all/channels/all/versions/all/releases"
            "?filter=version>130",
            timeout=3  # Very short timeout to fail fast
        )
        if response.status_code == 200:
            data = response.json()
            versions = []
            for release in data.get('releases', []):
                if release.get('serving', {}).get('servingPercentage', 0) > 0:
                    version = release.get('version', '')
                    if version and version not in versions:
                        versions.append(version)
            # Return top 5 most recent versions
            return sorted(versions, reverse=True)[:5]
    except Exception as e:
        # Only print on first call to avoid spam
        if not hasattr(get_real_chrome_versions, '_error_logged'):
            print(f"⚠️  Chrome version API unavailable, using fallback versions: {e}")
            get_real_chrome_versions._error_logged = True
    
    # Fallback list of recent stable Chrome versions (as of late 2024)
    return [
        "131.0.6778.85",
        "131.0.6778.69",
        "130.0.6723.116", 
        "130.0.6723.91",
        "129.0.6668.100"
    ]

def get_chrome_main_version() -> tuple[int, str]:
    """
    Detects the main version of Google Chrome installed on the system.
    Returns (major_version, full_version_string)
    """
    detected_version = None
    try:
        if platform.system() == "Windows":
            # The key can be in HKEY_CURRENT_USER or HKEY_LOCAL_MACHINE
            for root in (winreg.HKEY_CURRENT_USER, winreg.HKEY_LOCAL_MACHINE):
                try:
                    key = winreg.OpenKey(root, r"SOFTWARE\Google\Chrome\BLBeacon")
                    version, _ = winreg.QueryValueEx(key, "version")
                    detected_version = version
                    break
                except FileNotFoundError:
                    continue
        elif platform.system() == "Darwin": # macOS
            # Command to get chrome version on macOS
            process = subprocess.Popen(
                ['/Applications/Google Chrome.app/Contents/MacOS/Google Chrome', '--version'],
                stdout=subprocess.PIPE
            )
            version = process.communicate()[0].decode('UTF-8').strip().split()[-1]
            detected_version = version
        elif platform.system() == "Linux":
            # Command to get chrome version on Linux
            process = subprocess.Popen(['google-chrome', '--version'], stdout=subprocess.PIPE)
            version = process.communicate()[0].decode('UTF-8').strip().split()[-1]
            detected_version = version
    except Exception as e:
        print(f"Could not automatically detect Chrome version: {e}")
    
    # Get real Chrome versions from API
    real_versions = get_real_chrome_versions()
    
    if detected_version:
        major = int(detected_version.split('.')[0])
        # Try to find a real version with same major
        for v in real_versions:
            if v.startswith(f"{major}."):
                return major, v
        # If no match, use detected version
        return major, detected_version
    
    # Fallback: use most recent real version
    fallback_version = real_versions[0]
    fallback_major = int(fallback_version.split('.')[0])
    print(f"Using fallback Chrome version: {fallback_version}")
    return fallback_major, fallback_version

from .config import (
    CHROME_PROFILE_PATH,
    CACHE_DIR,
    LINKEDIN_USERNAME,
    LINKEDIN_PASSWORD,
    HEADLESS,
    BROWSER,
    COOKIE_FILE,
    CHROME_BINARY_PATH, CHROME_USER_DATA_DIR, CHROME_PROFILE_DIRECTORY, CHROME_DEBUG_PORT, FORCE_CLOSE_CHROME,
    PROXY, USE_DATA_IMPULSE, DI_USERNAME, DI_PASSWORD, DI_HOST, DI_PORT,
    GEO_ENFORCE, DI_COUNTRY, TZ_TOLERANCE_HOURS, DI_STICKY_SESSION, WARM_UP_MODE,
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
from .io_utils import get_local_timezone_offset_hours, choose_country_for_timezone

def _build_realistic_user_agent() -> str:
    try:
        major, full_version = get_chrome_main_version()
    except Exception:
        major, full_version = 131, "131.0.6778.85"  # Recent stable version
    sysname = platform.system()
    if sysname == "Windows":
        # Assume Windows 10, 64-bit
        return f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{full_version} Safari/537.36"
    if sysname == "Darwin":
        # Assume macOS 10.15.7 on Intel
        return f"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{full_version} Safari/537.36"
    # Linux default
    return f"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{full_version} Safari/537.36"

class NoGoodMatchFound(Exception):
    pass


class LinkedInScraper:
    def __init__(self, headless: bool = HEADLESS, skip_warmup: bool = False):
        # Set up comprehensive Chrome output suppression early
        self._setup_chrome_output_suppression()
        
        # First, detect real IP before any proxy setup
        self._real_ip_info = self._detect_real_ip()
        print(f"🔍 Real IP detected: {self._real_ip_info['ip']} | {self._real_ip_info['location']}")
        
        self._tmp_user_data_dir: Path | None = None
        self.driver = self._build_driver(headless)
        
        # Perform a one-time warm-up if this is a new profile session (unless skipped)
        if not skip_warmup:
            self._warm_up_profile()
        else:
            print("⏭️  Skipping browser profile warmup (--skip-warmup flag used)")

        self.wait = WebDriverWait(self.driver, 45)  # Extended for slow proxy
        
        # Counter for periodic proxy verification
        self._profiles_processed = 0
        self._proxy_check_interval = 20  # Check every 20 profiles
        self.openai = OpenAIIntegration()

    def _warm_up_profile(self):
        """
        Visits a few common websites to populate the browser profile with a more
        natural-looking history and cookie jar, avoiding a fingerprint that is
        exclusively focused on LinkedIn. This is a one-time "warm-up".
        """
        # A simple flag file to ensure warm-up only happens once per profile init.
        warm_up_flag = Path(CHROME_USER_DATA_DIR) / "warmup_complete.flag"
        if WARM_UP_MODE == "once" and warm_up_flag.exists():
            return
            
        print("🔥 Performing browser profile warm-up...")
        
        common_sites = [
            "https://www.google.com",
            "https://www.wikipedia.org",
            "https://www.bbc.com/news",
            "https://www.reddit.com",
            "https://news.ycombinator.com",
            "https://www.cnn.com",
            "https://www.espn.com",
        ]
        # Randomize and take a subset each warm-up
        random.shuffle(common_sites)
        warmup_sites = common_sites[:random.randint(3, 5)]
        # Log warmup start with selected sites (single line)
        try:
            self._log_event("warmup_start", {"sites": warmup_sites})
        except Exception:
            pass
        
        original_window = self.driver.current_window_handle
        
        for site in warmup_sites:
            try:
                print(f"    -> Visiting {site}")
                self.driver.switch_to.new_window('tab')
                self.driver.get(site)
                self._human_delay(2.0, 4.5)
                
                # Perform meaningful actions based on site
                if "google.com" in site:
                    # Try clicking "I'm Feeling Lucky" or do a search
                    try:
                        search_box = self.driver.find_element(By.NAME, "q")
                        queries = ["weather", "news today", "time zone", "stock market", "sports scores"]
                        self._human_type(search_box, random.choice(queries))
                        self._human_delay(0.5, 1.0)
                        search_box.send_keys(Keys.RETURN)
                        self._human_delay(2.0, 4.0)
                        # Click on a result
                        try:
                            results = self.driver.find_elements(By.CSS_SELECTOR, "h3")
                            if results and len(results) > 2:
                                self._human_click(results[random.randint(0, min(2, len(results)-1))])
                                self._human_delay(2.0, 4.0)
                        except Exception as e:
                                pass
                    except Exception as e:
                        pass
                
                elif "wikipedia.org" in site:
                    # Click on a random article link
                    try:
                        self._human_delay(1.0, 2.0)
                        article_links = self.driver.find_elements(By.CSS_SELECTOR, "a[href^='/wiki/']:not([class])")
                        if article_links:
                            random_link = random.choice(article_links[:20])  # Pick from first 20 links
                            self._human_click(random_link)
                            self._human_delay(2.0, 4.0)
                            # Light scroll on the article page
                            self._human_scroll_down(num_scrolls=random.randint(1, 3))
                    except Exception:
                        pass
                
                elif "reddit.com" in site:
                    # Browse a random subreddit
                    try:
                        popular_subs = ["/r/worldnews", "/r/technology", "/r/science", "/r/funny", "/r/pics"]
                        self.driver.get(f"https://www.reddit.com{random.choice(popular_subs)}")
                        self._human_delay(2.0, 4.0)
                        # Click on a post
                        try:
                            posts = self.driver.find_elements(By.CSS_SELECTOR, "[data-testid='post-container']")
                            if posts and len(posts) > 3:
                                self._human_click(posts[random.randint(0, min(3, len(posts)-1))])
                                self._human_delay(3.0, 5.0)
                                self._human_scroll_down(num_scrolls=random.randint(1, 2))
                        except Exception as e:
                            pass
                    except Exception as e:
                        print(e)
                        pass
                
                elif "bbc.com" in site or "cnn.com" in site:
                    # Click on a news article
                    try:
                        self._human_delay(1.0, 2.0)
                        article_links = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='/news/'], a[href*='/sport/'], a h3")
                        if article_links and len(article_links) > 3:
                            self._human_click(article_links[random.randint(0, min(3, len(article_links)-1))])
                            self._human_delay(3.0, 5.0)
                            self._human_scroll_down(num_scrolls=random.randint(2, 4))
                    except Exception:
                        pass

                else:
                    # Default behavior for other sites
                    self._human_scroll_down(num_scrolls=random.randint(2, 4))
                    self._human_delay(2.0, 4.0)
                
                # Additional random actions
                if random.random() < 0.3:
                    # Sometimes go back
                    try:
                        self.driver.back()
                        self._human_delay(1.0, 2.0)
                    except Exception as e:
                        print(e)
                        pass

            except Exception as e:
                print(e)
                print(f"      - Could not warm up with site {site}: {e}")
            finally:
                if len(self.driver.window_handles) > 1:
                    self.driver.close()
                    self.driver.switch_to.window(original_window)

        print("✅ Warm-up complete.")
        if WARM_UP_MODE == "once":
            warm_up_flag.touch()

    # ---------- Unified logging & network snapshot ----------
    def _log_event(self, event_type: str, data: dict) -> None:
        try:
            logs_dir = CACHE_DIR / "logs"
            logs_dir.mkdir(parents=True, exist_ok=True)
            record = {
                "ts": int(time.time()),
                "event": event_type,
                **(data or {}),
            }
            # Append to unified log file
            with open(logs_dir / "unified_log.jsonl", "a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, ensure_ascii=False) + "\n")
            # Also write a rolling latest.json for quick inspection
            with open(logs_dir / "latest.json", "w", encoding="utf-8") as fh2:
                json.dump(record, fh2, ensure_ascii=False)
        except Exception:
            pass

    def get_network_snapshot(self) -> dict:
        """Return current browsing IP and geo using out-of-band request."""
        snapshot: dict = {}
        try:
            # Build proxy dict for requests
            proxy_dict = {}
            if USE_DATA_IMPULSE:
                # Use the DataImpulse __cr format for country targeting
                auth_user = DI_USERNAME
                if getattr(self, '_proxy_country', None):
                    auth_user = f"{DI_USERNAME}__cr.{self._proxy_country}"
                elif GEO_ENFORCE and DI_COUNTRY:
                    auth_user = f"{DI_USERNAME}__cr.{DI_COUNTRY}"
                if DI_STICKY_SESSION:
                    auth_user += f";session.{DI_STICKY_SESSION}"
                proxy_auth = f"{auth_user}:{DI_PASSWORD}"
                proxy_url = f"http://{proxy_auth}@{DI_HOST}:{DI_PORT}"
                proxy_dict = {"http": proxy_url, "https": proxy_url}
            elif PROXY:
                proxy_dict = {"http": PROXY, "https": PROXY}
            
            # Get IP first
            response = requests.get("https://api.ipify.org/?format=json", proxies=proxy_dict, timeout=5)
            ip = response.json().get('ip')
            
            if ip:
                # Get geo details
                geo_response = requests.get(f"https://ipwho.is/{ip}", proxies=proxy_dict, timeout=5)
                data = geo_response.json()
                if data.get("success", True) is not False:
                    snapshot = {
                        "ip": data.get("ip"),
                        "country": data.get("country"),
                        "region": data.get("region"),
                        "city": data.get("city"),
                        "org": (data.get("connection") or {}).get("org"),
                        "asn": (data.get("connection") or {}).get("asn"),
                    }
                else:
                    snapshot = {"ip": ip}
        except Exception:
            snapshot = {}
        return snapshot

    def log_network_snapshot(self, meta: dict | None = None) -> None:
        snap = self.get_network_snapshot()
        merged = {**(meta or {}), **(snap or {})}
        self._log_event("network", merged)
        if snap.get("ip"):
            print(f"🌐 Network snapshot — IP: {snap.get('ip')} | {snap.get('country') or ''} {snap.get('region') or ''} {snap.get('city') or ''}")

    # ---------- "Humanization" Toolkit ----------
    def _human_delay(self, lo=0.8, hi=2.5):
        """Waits for a random duration to mimic human thinking/reading time."""
        time.sleep(random.uniform(lo, hi))

    def _human_type(self, element: WebElement, text: str, log_label: str | None = None, lo: float = 0.05, hi: float = 0.15):
        """Types text into an element one character at a time with random delays.
        Logs average inter-key delay if log_label is provided (at start)."""
        # Precompute per-key delays to log average before typing begins
        delays = [random.uniform(lo, hi) for _ in text]
        if log_label:
            avg_ms = int(1000 * (sum(delays) / (len(delays) or 1)))
            try:
                self._log_event("typing_start", {"label": log_label, "chars": len(text or ""), "avg_delay_ms": avg_ms})
            except Exception:
                pass
        for idx, char in enumerate(text):
            element.send_keys(char)
            time.sleep(delays[idx] if idx < len(delays) else random.uniform(lo, hi))

    def _click_and_hold(self, element: WebElement, duration: float):
        """Clicks and holds the mouse button on an element for a given duration."""
        ac = ActionChains(self.driver)
        ac.click_and_hold(element).pause(duration).release().perform()

    def _open_in_new_tab_human_like(self, element: WebElement):
        """
        Opens a link in a new tab by simulating a CTRL+click or CMD+click,
        which is a more human-like behavior than using window.open().
        """
        try:
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});", element)
            self._human_delay(0.1, 0.3)
        except Exception:
            pass
        # Determine the correct key to press based on the operating system
        modifier_key = Keys.COMMAND if platform.system() == "Darwin" else Keys.CONTROL

        # Perform the key-down, click, and key-up sequence
        ac = ActionChains(self.driver)
        ac.key_down(modifier_key).click(element).key_up(modifier_key).perform()

    def _get_bezier_points(self, start, end, control, steps=20):
        """Calculates points for a quadratic Bezier curve."""
        points = []
        for i in range(steps + 1):
            t = i / steps
            x = (1 - t)**2 * start[0] + 2 * (1 - t) * t * control[0] + t**2 * end[0]
            y = (1 - t)**2 * start[1] + 2 * (1 - t) * t * control[1] + t**2 * end[1]
            points.append((x, y))
        return points

    def _move_and_click(self, element: WebElement):
        """
        Moves the mouse in a human-like Bezier curve to the element and clicks.
        This is a highly advanced method for emulating human interaction.
        """
        try:
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});", element)
            self._human_delay(0.1, 0.3)
            
            # Get element position and size
            location = element.location
            size = element.size
            start_x = random.randint(location['x'], location['x'] + size['width'])
            start_y = random.randint(location['y'], location['y'] + size['height'])
            
            # Define start, end, and a random control point for the curve
            start_pos = (start_x, start_y)
            end_pos = (location['x'] + size['width'] // 2, location['y'] + size['height'] // 2)
            control_x = (start_x + end_pos[0]) / 2 + random.randint(-50, 50)
            control_y = (start_y + end_pos[1]) / 2 + random.randint(-50, 50)
            control_pos = (control_x, control_y)
            
            points = self._get_bezier_points(start_pos, end_pos, control_pos)
            
            ac = ActionChains(self.driver)
            ac.move_to_element(element) # Initial move to get in the general area
            
            # Move along the curve
            for i in range(len(points) - 1):
                ac.move_by_offset(points[i+1][0] - points[i][0], points[i+1][1] - points[i][1])
                ac.pause(random.uniform(0.005, 0.02))
            
            ac.pause(random.uniform(0.1, 0.3)).click().perform()

        except Exception as e:
            print(f"    ... Bezier mouse movement failed ({e}), falling back to robust force click.")
            self._force_click(element)

    def _perform_innocent_action(self):
        """
        Performs a random 'innocent' action in a new tab to avoid disrupting the main
        scraping flow, making the scraper's behavior more human-like and robust.
        """
        print("🕊️ Performing a random innocent action in a new tab...")
        original_window = self.driver.current_window_handle
        
        try:
            # Open and switch to a new tab
            self.driver.switch_to.new_window('tab')
            
            actions = [
                self._innocent_action_scroll_feed,
                self._innocent_action_view_network,
                self._innocent_action_view_notifications,
                self._innocent_action_view_own_profile,
            ]
            
            # Choose and execute a random action in the new tab
            random.choice(actions)()

        except Exception as e:
            print(f"    ⚠️  Innocent action failed: {e}")
        finally:
            # Ensure we always close the new tab and switch back
            if len(self.driver.window_handles) > 1:
                self.driver.close()
            self.driver.switch_to.window(original_window)
            self._human_delay(0.5, 1.0) # Small delay to ensure context switch is stable
            print("    -> Resumed main task.")

    def _innocent_action_scroll_feed(self):
        print("    -> Innocent action: Scrolling the main feed.")
        self.driver.get("https://www.linkedin.com/feed/")
        self._ensure_linkedin_script_injected()
        self._human_delay(2, 4)
        
        # More robust feed interaction
        total_time = random.uniform(15, 30)  # Spend 15-30 seconds on feed
        start_time = time.time()
        
        while time.time() - start_time < total_time:
            # Scroll with varying speeds and distances
            num_scrolls = random.randint(1, 3)
            self._human_scroll_down(num_scrolls=num_scrolls, delay_between=random.uniform(0.3, 1.2))
            self._human_delay(2, 5)
            
            # Sometimes interact with posts
            if random.random() < 0.3:
                try:
                    # Find reaction buttons (like, celebrate, etc.)
                    reaction_buttons = self.driver.find_elements(By.CSS_SELECTOR, "button[aria-label*='React']")
                    if reaction_buttons and len(reaction_buttons) > 2:
                        # Hover over a post first
                        post_element = reaction_buttons[random.randint(0, min(3, len(reaction_buttons)-1))]
                        ActionChains(self.driver).move_to_element(post_element).pause(random.uniform(0.5, 1.5)).perform()
                        self._human_delay(0.5, 1.5)
                except Exception:
                    pass
            
            # Sometimes read comments
            if random.random() < 0.2:
                try:
                    comment_buttons = self.driver.find_elements(By.CSS_SELECTOR, "button[aria-label*='Comment']")
                    if comment_buttons:
                        self._human_click(comment_buttons[0])
                        self._human_delay(2, 4)
                        # Scroll through comments
                        self._human_scroll_down(num_scrolls=random.randint(1, 2))
                        self._human_delay(1, 3)
                except Exception:
                    pass
            
            # Occasional pause to "read"
            if random.random() < 0.4:
                self._human_delay(3, 7)

    def _innocent_action_view_network(self):
        print("    -> Innocent action: Viewing 'My Network'.")
        self.driver.get("https://www.linkedin.com/mynetwork/")
        self._ensure_linkedin_script_injected()
        self._human_delay(2.5, 5)
        
        # Spend more time exploring network
        total_time = random.uniform(10, 20)
        start_time = time.time()
        
        while time.time() - start_time < total_time:
            self._human_scroll_down(num_scrolls=random.randint(1, 2))
            self._human_delay(2, 4)
            
            # Sometimes hover over connection suggestions
            if random.random() < 0.3:
                try:
                    suggestion_cards = self.driver.find_elements(By.CSS_SELECTOR, "div[data-test-invitation-card]")
                    if suggestion_cards:
                        card = random.choice(suggestion_cards[:5])
                        ActionChains(self.driver).move_to_element(card).pause(random.uniform(1, 2)).perform()
                        self._human_delay(0.5, 1.5)
                except Exception as e:
                    print(e)
                    pass

            # Sometimes click on "People you may know" tabs
            if random.random() < 0.2:
                try:
                    tabs = self.driver.find_elements(By.CSS_SELECTOR, "button[role='tab']")
                    if tabs and len(tabs) > 1:
                        random_tab = random.choice(tabs[1:])  # Skip first tab
                        self._human_click(random_tab)
                        self._human_delay(2, 4)
                except Exception:
                    pass
        
    def _innocent_action_view_notifications(self):
        print("    -> Innocent action: Viewing notifications.")
        self.driver.get("https://www.linkedin.com/notifications/")
        self._ensure_linkedin_script_injected()
        self._human_delay(3, 6) # Users spend more time on notifications
        
        # Read through notifications more thoroughly
        total_time = random.uniform(10, 25)
        start_time = time.time()
        
        while time.time() - start_time < total_time:
            # Scroll through notifications
            self._human_scroll_down(num_scrolls=random.randint(1, 2), delay_between=random.uniform(0.5, 1.5))
            self._human_delay(2, 5)
            
            # Sometimes click on a notification to expand/read
            if random.random() < 0.3:
                try:
                    notifications = self.driver.find_elements(By.CSS_SELECTOR, "div[data-finite-scroll-hotkey-item]")
                    if notifications and len(notifications) > 2:
                        notif = notifications[random.randint(0, min(4, len(notifications)-1))]
                        self._human_click(notif)
                        self._human_delay(2, 4)
                        # Sometimes go back
                        if random.random() < 0.5:
                            self.driver.back()
                            self._human_delay(1, 2)
                except Exception:
                    pass
            
            # Filter notifications
            if random.random() < 0.15:
                try:
                    filter_buttons = self.driver.find_elements(By.CSS_SELECTOR, "button[aria-label*='Filter']")
                    if filter_buttons:
                        self._human_click(filter_buttons[0])
                        self._human_delay(1, 3)
                except Exception:
                    pass

    def _innocent_action_view_own_profile(self):
        """
        Innocent action: Navigates to the user's own profile page.
        Simplified to direct navigation for robustness in a new tab.
        """
        print("    -> Innocent action: Viewing own profile.")
        self.driver.get("https://www.linkedin.com/in/me/") # A more stable URL
        self._ensure_linkedin_script_injected()
        self._human_delay(2, 4)
        
        # Spend more time on own profile
        total_time = random.uniform(8, 15)
        start_time = time.time()
        
        while time.time() - start_time < total_time:
            self._human_scroll_down(num_scrolls=random.randint(1, 2))
            self._human_delay(2, 4)
            
            # Sometimes check profile sections
            if random.random() < 0.25:
                try:
                    # Click on "Show all" buttons for various sections
                    show_all_buttons = self.driver.find_elements(By.CSS_SELECTOR, "button[aria-label*='Show all'], a[aria-label*='Show all']")
                    if show_all_buttons:
                        button = random.choice(show_all_buttons[:3])
                        self._human_click(button)
                        self._human_delay(2, 4)
                        # Go back after viewing
                        self.driver.back()
                        self._human_delay(1, 2)
                except Exception:
                    pass
            
            # Sometimes hover over profile sections
            if random.random() < 0.3:
                try:
                    sections = self.driver.find_elements(By.CSS_SELECTOR, "section[class*='artdeco-card']")
                    if sections and len(sections) > 2:
                        section = random.choice(sections[:5])
                        ActionChains(self.driver).move_to_element(section).pause(random.uniform(0.5, 1.5)).perform()
                except Exception:
                    pass

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
            # Try simple element click as fallback (still produces trusted events)
            try:
                element.click()
            except Exception:
                    # If element click also fails, try scrolling into view first
                try:
                        self.driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});", element)
                        self._human_delay(0.2, 0.4)
                        element.click()
                except Exception:
                        # As last resort, try ActionChains without offset
                        ActionChains(self.driver).move_to_element(element).pause(0.1).click().perform()



    def _human_scroll_down(self, num_scrolls=3, delay_between=0.4):
        """Scrolls down the page in a series of key presses."""
        body = self.driver.find_element(By.TAG_NAME, 'body')
        for _ in range(num_scrolls):
            body.send_keys(Keys.PAGE_DOWN)
            time.sleep(random.uniform(delay_between, delay_between + 0.3))



    # ---------- driver helpers ----------
    def _setup_seleniumwire_ssl_certificate(self):
        """
        Set up selenium-wire SSL certificate for genuine HTTPS security.
        Based on the working test_seleniumwire_ssl_combined.py configuration.
        """
        try:
            import subprocess
            import tempfile
            
            print("🔧 Setting up selenium-wire SSL certificate for genuine security...")
            
            # Step 1: Extract selenium-wire's certificate
            result = subprocess.run([
                sys.executable, '-m', 'seleniumwire', 'extractcert'
            ], capture_output=True, text=True, cwd=os.getcwd())
            
            if result.returncode == 0:
                cert_path = os.path.join(os.getcwd(), 'ca.crt')
                if os.path.exists(cert_path):
                    print(f"✅ Selenium-wire certificate extracted")
                    
                    # Step 2: Install certificate in user's certificate store
                    cmd = ['certutil', '-user', '-addstore', '-f', 'Root', cert_path]
                    install_result = subprocess.run(cmd, capture_output=True, text=True)
                    
                    if install_result.returncode == 0:
                        print(f"✅ SSL certificate installed - genuine HTTPS security enabled")
                        
                        # Clean up certificate file
                        try:
                            os.remove(cert_path)
                        except:
                            pass
                        
                        return True
                    else:
                        print(f"⚠️  Certificate installation failed (may require manual installation)")
                        print(f"   Certificate file saved to: {cert_path}")
                        return False
                else:
                    print(f"❌ Certificate file not found after extraction")
                    return False
            else:
                print(f"❌ Certificate extraction failed: {result.stderr}")
                return False
                
        except Exception as e:
            print(f"⚠️  SSL certificate setup failed: {e}")
            print(f"   Continuing with basic SSL handling")
            return False

    def _build_driver(self, headless: bool):
        """
        Builds a Selenium driver instance with Chrome proxy extension for authentication.
        Includes timezone-based country selection for optimal proxy location.
        """
        from .config import CHROME_PROFILE_PATH  # import inside to avoid cycles

        # Initialize proxy configuration
        if USE_DATA_IMPULSE and SELENIUM_WIRE_AVAILABLE:
            # Use selenium-wire for DataImpulse (proven to work!)
            print("🔌 Using selenium-wire for DataImpulse proxy authentication")
            
            # Set up SSL certificate handling for genuine HTTPS security
            self._setup_seleniumwire_ssl_certificate()
            
            # Determine country if GEO_ENFORCE enabled and DI_COUNTRY unset
            country = DI_COUNTRY
            if GEO_ENFORCE and not country:
                local_off = get_local_timezone_offset_hours()
                inferred = choose_country_for_timezone(local_off)
                country = inferred or "us"
                print(f"🌍 Auto-selected proxy country '{country}' for timezone UTC{local_off:+d}")
            
            # Store the effective country for proxy verification
            self._proxy_country = country
            
            # Build auth username with DataImpulse format
            auth_user = DI_USERNAME
            if country:
                auth_user = f"{DI_USERNAME}__cr.{country}"
            if DI_STICKY_SESSION:
                auth_user += f";session.{DI_STICKY_SESSION}"
            
            # Create proxy URL for selenium-wire
            proxy_url = f"http://{auth_user}:{DI_PASSWORD}@{DI_HOST}:{DI_PORT}"
            
            seleniumwire_options = {
                "proxy": {
                    "http": proxy_url,
                    "https": proxy_url,
                },
                # SSL options to prevent strikethrough HTTPS
                "verify_ssl": True,
                "suppress_connection_errors": True,  # Suppress ConnectionAbortedError
                "connection_keep_alive": False,  # Disable keep-alive to reduce connection errors
                "disable_encoding": True,  # Disable encoding to reduce connection issues
                "backend": "default",  # Use default backend for stability
                # Add options to handle malformed responses
                "ignore_http_methods": [],  # Capture all methods
                "request_storage": "memory",  # Use memory storage
                "request_storage_max_size": 100,  # Limit stored requests
                # Disable response body decoding to avoid gzip errors
                "disable_capture": False,
                "enable_har": False,  # Disable HAR to reduce processing
            }
            
            print(f"✅ Configured selenium-wire proxy:")
            print(f"   Host: {DI_HOST}:{DI_PORT}")
            print(f"   Auth User: {auth_user}")
            print(f"   Country: {country or 'default'}")
            
            # Create Chrome options for selenium-wire
            options = wire_webdriver.ChromeOptions()
            options.add_argument(f'--user-data-dir={CHROME_USER_DATA_DIR}')
            options.add_argument(f'--profile-directory={CHROME_PROFILE_DIRECTORY}')
            
            # Minimal automation detection bypass (Chrome 138 compatible)
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--disable-infobars')
            
            # Conservative console suppression for WebGL and TensorFlow errors
            options.add_argument('--log-level=4')  # Suppress INFO, WARNING, ERROR messages  
            options.add_argument('--disable-logging')
            options.add_argument('--silent')
            options.add_argument('--disable-gpu')  # Essential for suppressing GPU/WebGL errors
            options.add_argument('--enable-unsafe-swiftshader')  # Suppress WebGL fallback warnings
            
            # Suppress TensorFlow/ABSL messages and ML features  
            options.add_argument('--disable-features=VizDisplayCompositor,AudioServiceOutOfProcess')
            options.add_argument('--disable-component-extensions-with-background-pages')
            options.add_argument('--disable-background-networking')
            options.add_argument('--disable-background-timer-throttling')
            options.add_argument('--disable-backgrounding-occluded-windows')
            options.add_argument('--disable-ipc-flooding-protection')
            
            # Disable ML/AI features that generate TensorFlow messages
            options.add_argument('--disable-features=MediaFoundationClearPlayback')
            options.add_argument('--disable-features=VoiceTranscription')
            options.add_argument('--disable-features=OptimizationHints')
            options.add_argument('--disable-features=TranslateUI')
            options.add_argument('--disable-component-update')
            
            # Comprehensive logging suppression - catch-all for Chrome internal messages
            options.add_argument('--disable-logging')
            options.add_argument('--log-level=3')
            options.add_argument('--silent')
            options.add_argument('--disable-features=VoiceRecognition,SpeechSynthesis')
            options.add_argument('--disable-features=MediaSessionService,AudioServiceSandbox')
            options.add_argument('--use-fake-device-for-media-stream')
            options.add_argument('--disable-audio-input')
            options.add_argument('--disable-audio-output')
            
            # Suppress histograms, metrics, and worker logging

            options.add_argument('--disable-features=MediaRouter,CalculateNativeWinOcclusion')
            options.add_argument('--disable-features=InterestFeedContentSuggestions')
            options.add_argument('--disable-client-side-phishing-detection')
            options.add_argument('--disable-default-apps')
            options.add_argument('--disable-domain-reliability')
            options.add_argument('--disable-sync')
            options.add_argument('--disable-features=AutofillServerCommunication')
            options.add_argument('--disable-features=CertificateTransparencyComponentUpdater')
            
            # Specifically target histogram and metrics reporting (the root cause of verbose output)
            options.add_argument('--disable-features=UserActivityService,UserEducationService')
            options.add_argument('--disable-features=MetricsReporting,UkmService')
            options.add_argument('--disable-histogram-customization')
            options.add_argument('--disable-background-mode')
            options.add_argument('--disable-breakpad')
            options.add_argument('--disable-component-cloud-policy')
            options.add_argument('--no-reporting')
            options.add_argument('--no-crash-upload')
            options.add_argument('--disable-field-trial-config')
            options.add_argument('--disable-variations')
            
            # Basic stability arguments (matching working test)
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--window-size=1200,800')
            
            # NO SSL-bypassing arguments - use genuine SSL verification
            # Certificate is installed, so SSL should work properly
            
            # Keep minimal security arguments - avoid obvious bot flags
            # Remove aggressive SSL disabling to maintain stealth
            
            # Minimal Chrome preferences - only disable notifications to reduce distractions
            prefs = {
                "profile.default_content_setting_values": {
                    "notifications": 2,  # Disable notifications only
                }
            }
            options.add_experimental_option("prefs", prefs)
            
            if headless:
                options.add_argument('--headless=new')
            
            # Create driver with selenium-wire (with retry logic for Windows)
            max_retries = 3
            retry_delay = 2
            driver = None
            
            for attempt in range(max_retries):
                try:
                    # Add a small delay between attempts to avoid socket conflicts
                    if attempt > 0:
                        import time
                        time.sleep(retry_delay)
                        print(f"🔄 Main driver retry attempt {attempt + 1}/{max_retries}...")
                    
                    driver = wire_webdriver.Chrome(
                        options=options,
                        seleniumwire_options=seleniumwire_options
                    )
                    print(f"✅ Selenium-wire Chrome driver created successfully (attempt {attempt + 1})")
                    break
                except Exception as e:
                    error_type = type(e).__name__
                    error_msg = str(e)
                    print(f"❌ Main driver attempt {attempt + 1} failed: {error_type}: {error_msg}")
                    
                    # Provide specific guidance for common errors
                    if "crashed" in error_msg.lower():
                        print("💡 Chrome crashed - this may be due to incompatible arguments or resource constraints")
                    elif "reachable" in error_msg.lower():
                        print("💡 Chrome not reachable - check if Chrome is properly installed")
                    elif "timeout" in error_msg.lower():
                        print("💡 Timeout error - Chrome may be taking too long to start")
                    
                    if attempt == max_retries - 1:
                        print(f"❌ All selenium-wire attempts failed, falling back to standard driver...")
                        try:
                            driver = self._create_fallback_driver(headless)
                        except Exception as fallback_error:
                            print(f"❌ Fallback driver also failed: {type(fallback_error).__name__}: {fallback_error}")
                            raise Exception(f"Unable to create any Chrome driver. Last error: {fallback_error}")
                    else:
                        # Clean up any partial connections
                        try:
                            import psutil
                            # Kill any hanging chrome processes
                            for proc in psutil.process_iter(['pid', 'name']):
                                if 'chrome' in proc.info['name'].lower():
                                    try:
                                        proc.terminate()
                                    except:
                                        pass
                        except:
                            pass
            
        elif PROXY:
            # Simple proxy without authentication
            print(f"🔌 Configuring simple proxy: {PROXY}")
            driver = self._create_simple_proxy_driver(headless)
            
        else:
            # No proxy - standard undetected chrome
            driver = self._create_standard_driver(headless)
        
        # Apply CDP overrides for all driver types
        try:
            ua_string = _build_realistic_user_agent()
            major_version, full_version = get_chrome_main_version()
            
            sysname = platform.system()
            if sysname == "Windows":
                platform_name = "Windows"
                platform_version = "15.0.0"
                architecture = "x86"
            elif sysname == "Darwin":
                platform_name = "macOS"
                platform_version = "13.0.0"
                architecture = "x86"
            else:
                platform_name = "Linux"
                platform_version = "6.0.0"
                architecture = "x86"
            
            brands = [
                {"brand": "Chromium", "version": str(major_version)},
                {"brand": "Google Chrome", "version": str(major_version)},
                {"brand": "Not.A/Brand", "version": "99"},
            ]
            full_version_list = [
                {"brand": "Chromium", "version": full_version},
                {"brand": "Google Chrome", "version": full_version},
                {"brand": "Not.A/Brand", "version": "99.0.0.0"},
            ]
            
            metadata = {
                "brands": brands,
                "fullVersion": full_version,
                "fullVersionList": full_version_list,
                "platform": platform_name,
                "platformVersion": platform_version,
                "architecture": architecture,
                "model": "",
                "mobile": False,
            }
            
            driver.execute_cdp_cmd("Network.enable", {})
            driver.execute_cdp_cmd("Network.setUserAgentOverride", {
                "userAgent": ua_string,
                "userAgentMetadata": metadata,
            })
            driver.execute_cdp_cmd("Network.setExtraHTTPHeaders", {
                "headers": {"Accept-Language": "en-US,en;q=0.9"}
            })
            
            # Minimal automation detection bypass using CDP (Chrome 138 compatible)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': '''
                    // Only hide navigator.webdriver - keep everything else natural
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined,
                        configurable: true,
                    });
                '''
            })
        except Exception:
            pass
        
        driver.maximize_window()
        
        # Set up page navigation listener to inject anti-tracking script on LinkedIn pages
        self._setup_linkedin_page_listener(driver)
        
        print("✅ LinkedIn page listener configured for anti-tracking injection.")
        
        # Set extended timeouts for slow proxy performance
        driver.set_page_load_timeout(120)  # Extended from default 30s
        driver.implicitly_wait(15)  # Extended from default 0s
        
        self._verify_proxy()
        return driver

    def _setup_chrome_output_suppression(self):
        """
        Set up comprehensive environment variables and system settings to suppress
        ALL Chrome output including histograms, worker logs, and verbose messages.
        """
        try:
            import os
            
            # Set Chrome-specific environment variables to suppress all output
            null_device = 'NUL' if platform.system() == 'Windows' else '/dev/null'
            
            # Chrome logging environment variables
            os.environ['CHROME_LOG_FILE'] = null_device
            os.environ['CHROME_SILENCE_WARNINGS'] = '1'
            os.environ['CHROME_DISABLE_GPU_LOGGING'] = '1'
            
            # Suppress Chromium histogram and metrics logging
            os.environ['DISABLE_HISTOGRAMS'] = '1'
            os.environ['CHROME_DISABLE_HISTOGRAM_CUSTOMIZATION'] = '1'
            os.environ['DISABLE_CHROMIUM_METRICS'] = '1'
            
            # Suppress worker and renderer process logging
            os.environ['CHROME_DISABLE_WORKER_LOGGING'] = '1'
            os.environ['DISABLE_V8_COMPILE_CACHE'] = '1'
            
            # General logging suppression
            os.environ['PYTHONHASHSEED'] = '0'  # Reduce Python-level verbosity
            
            print("🤐 Comprehensive Chrome output suppression enabled")
            
        except Exception as e:
            print(f"⚠️  Could not set all environment variables: {e}")
            pass

    def _create_fallback_driver(self, headless: bool):
        """Create fallback undetected chrome driver without proxy"""
        print("🔄 Creating fallback driver without proxy")
        options = uc.ChromeOptions()
        options.add_argument(f'--user-data-dir={CHROME_USER_DATA_DIR}')
        options.add_argument(f'--profile-directory={CHROME_PROFILE_DIRECTORY}')
        options.add_argument('--disable-blink-features=AutomationControlled')
        
        # Add conservative logging suppression
        options.add_argument('--log-level=3')
        options.add_argument('--disable-logging')
        options.add_argument('--silent')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-features=VoiceTranscription,OptimizationHints,TranslateUI')
        options.add_argument('--disable-component-update')
            
        if headless:
            options.add_argument('--headless=new')
            
        try:
            major_version, _ = get_chrome_main_version()
            return uc.Chrome(options=options, version_main=major_version, service_log_path='NUL' if platform.system() == 'Windows' else '/dev/null')
        except Exception:
            return uc.Chrome(options=options, service_log_path='NUL' if platform.system() == 'Windows' else '/dev/null')

    

    def _create_simple_proxy_driver(self, headless: bool):
        """Create driver with simple proxy"""
        options = uc.ChromeOptions()
        options.add_argument(f'--user-data-dir={CHROME_USER_DATA_DIR}')
        options.add_argument(f'--profile-directory={CHROME_PROFILE_DIRECTORY}')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument(f'--proxy-server={PROXY}')
        
        # Add conservative logging suppression
        options.add_argument('--log-level=3')
        options.add_argument('--disable-logging')
        options.add_argument('--silent')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-features=VoiceTranscription,OptimizationHints,TranslateUI')
        options.add_argument('--disable-component-update')
        
        if headless:
            options.add_argument('--headless=new')
            
        try:
            major_version, _ = get_chrome_main_version()
            return uc.Chrome(options=options, version_main=major_version, service_log_path='NUL' if platform.system() == 'Windows' else '/dev/null')
        except Exception:
            return uc.Chrome(options=options, service_log_path='NUL' if platform.system() == 'Windows' else '/dev/null')

    def _create_standard_driver(self, headless: bool):
        """Create standard undetected chrome driver"""
        options = uc.ChromeOptions()
        options.add_argument(f'--user-data-dir={CHROME_USER_DATA_DIR}')
        options.add_argument(f'--profile-directory={CHROME_PROFILE_DIRECTORY}')
        options.add_argument('--disable-blink-features=AutomationControlled')
        
        # Add conservative logging suppression
        options.add_argument('--log-level=3')
        options.add_argument('--disable-logging')
        options.add_argument('--silent')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-features=VoiceTranscription,OptimizationHints,TranslateUI')
        options.add_argument('--disable-component-update')
        
        if headless:
            options.add_argument('--headless=new')
            
        try:
            major_version, _ = get_chrome_main_version()
            return uc.Chrome(options=options, version_main=major_version, service_log_path='NUL' if platform.system() == 'Windows' else '/dev/null')
        except Exception:
            return uc.Chrome(options=options, service_log_path='NUL' if platform.system() == 'Windows' else '/dev/null')

    def _setup_linkedin_page_listener(self, driver):
        """Setup a page listener that injects anti-tracking script when LinkedIn pages load."""
        # Enable Page domain events
        driver.execute_cdp_cmd("Page.enable", {})
        
        # Add the script injection listener
        def page_listener(event_name, event_data):
            if event_name == 'Page.frameNavigated':
                frame = event_data.get('frame', {})
                url = frame.get('url', '')
                if 'linkedin.com' in url:
                    try:
                        # Small delay to let page start loading
                        time.sleep(0.1)
                        # Injection method now handles its own logging
                        self._inject_linkedin_anti_tracking_script(driver)
                    except Exception as e:
                        print(f"⚠️  Failed to inject anti-tracking script: {e}")
                        self._log_event("anti_tracking_injection", {
                            "success": False,
                            "error": str(e),
                            "url": url,
                            "context": "page_listener"
                        })
        
        # Store the listener reference for potential cleanup
        self._page_listener = page_listener
        
        # Note: CDP listener setup would be complex here, so we'll use a simpler approach
        # by checking and injecting on each major LinkedIn navigation method

    def _inject_linkedin_anti_tracking_script(self, driver):
        """Inject the anti-tracking and spoofing script specifically for LinkedIn pages."""
        js_script = """
        (function() {
            // Only run on LinkedIn
            if (!window.location.hostname.includes('linkedin.com')) return false;
            
            // Skip if already injected
            if (window.__linkedin_antitrack_injected) return true;
            window.__linkedin_antitrack_injected = true;
            
            // Track blocked scripts
            window.__linkedin_tracking_blocked = 0;
            
            // Prevent loading of LinkedIn's tracking/fingerprint script
            const stripedSource = "snap.licdn.com/li.lms-analytics/insight.min.js";
            const observeScript = new MutationObserver((mutations) => {
                for (const m of mutations) {
                    for (const n of m.addedNodes) {
                        if (n.tagName === 'SCRIPT' && n.src.includes(stripedSource)) {
                            // Remove tracking script and increment counter
                            n.remove();
                            window.__linkedin_tracking_blocked++;
                        }
                    }
                }
            });
            observeScript.observe(document.documentElement, { childList: true, subtree: true });

            // Spoof minimal WebGL parameters (renderer/vendor) without breaking functionality
            const platform = navigator.platform || 'Unknown';
            const spoofWebGL = function(context) {
              const getParameter = context.prototype.getParameter;
              context.prototype.getParameter = function(param){
              if (param === 37445) { /* UNMASKED_VENDOR_WEBGL */
                return 'Google Inc.';
              }
              if (param === 37446) { /* UNMASKED_RENDERER_WEBGL */
                  // Platform-specific renderer strings
                  if (platform.indexOf('Win') !== -1) {
                    return 'ANGLE (Intel(R) UHD Graphics Direct3D11 vs_5_0 ps_5_0)';
                  } else if (platform.indexOf('Mac') !== -1) {
                    return 'ANGLE (Intel Iris Pro OpenGL Engine)';
                  } else if (platform.indexOf('Linux') !== -1) {
                    return 'ANGLE (Intel Open Source Technology Center Mesa DRI Intel(R) HD Graphics)';
                  } else {
                    return 'ANGLE (Unknown Renderer)';
                  }
              }
              try { return getParameter.apply(this, arguments); } catch(e) { return null; }
              };
            };
            
            // Apply to WebGLRenderingContext
            if (window.WebGLRenderingContext) spoofWebGL(WebGLRenderingContext);
            
            // Apply to WebGL2RenderingContext if it exists
            if (typeof WebGL2RenderingContext !== 'undefined') {
              spoofWebGL(WebGL2RenderingContext);
            }
            
            // Handle OffscreenCanvas if supported
            if (typeof OffscreenCanvas !== 'undefined') {
              const originalGetContext = OffscreenCanvas.prototype.getContext;
              OffscreenCanvas.prototype.getContext = function(contextType, ...args) {
                const context = originalGetContext.apply(this, [contextType, ...args]);
                
                // Add noise to offscreen canvas operations
                if (contextType === '2d' && context) {
                  const getImageData = context.getImageData;
                  if (getImageData) {
                    context.getImageData = function(sx, sy, sw, sh) {
                      const imageData = getImageData.apply(this, arguments);
                      // Add subtle noise
                      for (let i = 0; i < imageData.data.length; i += 4) {
                        if (Math.random() < 0.01) {
                          imageData.data[i] = Math.min(255, Math.max(0, imageData.data[i] + (Math.random() < 0.5 ? -1 : 1)));
                        }
                      }
                      return imageData;
                    };
                  }
                }
                
                return context;
              };
            }

            // Hide webdriver in multiple places
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            
            // Canvas fingerprint noise
            const injectNoise = function() {
              if (window.CanvasRenderingContext2D) {
                const getImageData = CanvasRenderingContext2D.prototype.getImageData;
                CanvasRenderingContext2D.prototype.getImageData = function(sx, sy, sw, sh) {
                  const imageData = getImageData.apply(this, arguments);
                  // Add very subtle noise to canvas data
                  for (let i = 0; i < imageData.data.length; i += 4) {
                    // Only modify ~1% of pixels with tiny changes
                    if (Math.random() < 0.01) {
                      imageData.data[i] = Math.min(255, Math.max(0, imageData.data[i] + (Math.random() < 0.5 ? -1 : 1)));
                    }
                  }
                  return imageData;
                };
                
                // Also add noise to toDataURL
                const toDataURL = HTMLCanvasElement.prototype.toDataURL;
                HTMLCanvasElement.prototype.toDataURL = function() {
                  // Add tiny random pixel in corner before converting
                  const ctx = this.getContext('2d');
                  if (ctx) {
                    const imageData = ctx.getImageData(0, 0, 1, 1);
                    imageData.data[3] = Math.floor(Math.random() * 256); // Modify alpha channel slightly
                    ctx.putImageData(imageData, 0, 0);
                  }
                  return toDataURL.apply(this, arguments);
                };
              }
            };
            injectNoise();
            
            return true; // Successfully injected
          })();
        """
        
        injection_success = False
        method_used = "none"
        
        # Enhanced debugging variables
        page_ready = False
        script_error = None
        cdp_error = None
        
        try:
            # Check if we're on a valid page first
            try:
                current_url = driver.current_url
                if not current_url or current_url == "data:,":
                    script_error = f"Invalid page URL: {current_url}"
                    return False  # No valid page loaded
                page_ready = True
            except Exception as url_error:
                script_error = f"Could not get current URL: {type(url_error).__name__}: {url_error}"
                return False
                
            # Try standard script execution first
            try:
                result = driver.execute_script(js_script)
                if result:
                    injection_success = True
                    method_used = "execute_script"
                else:
                    script_error = "Script executed but returned falsy result"
            except Exception as e:
                script_error = f"execute_script failed: {type(e).__name__}: {e}"
                
        except Exception as e:
            script_error = f"Unexpected error in main try block: {type(e).__name__}: {e}"
            
        # Fallback: try CDP injection if main method failed
        if not injection_success:
            try:
                driver.execute_cdp_cmd("Runtime.evaluate", {"expression": js_script})
                # Since CDP doesn't return the result, we need to check if it worked
                verification_result = driver.execute_script("return window.__linkedin_antitrack_injected || false;")
                if verification_result:
                    injection_success = True
                    method_used = "cdp"
                else:
                    cdp_error = "CDP execution succeeded but verification returned false"
            except Exception as cdp_exception:
                cdp_error = f"CDP injection failed: {type(cdp_exception).__name__}: {cdp_exception}"
                injection_success = False
                method_used = "failed"
                
        # Get current URL for logging context
        current_url = ""
        try:
            current_url = driver.current_url
        except Exception:
            current_url = "unknown"
        
        # Check for blocked tracking scripts (with a small delay to let MutationObserver work)
        blocked_count = 0
        try:
            time.sleep(0.1)  # Brief pause to let script work
            blocked_count = driver.execute_script("return window.__linkedin_tracking_blocked || 0;")
        except Exception:
            pass
        
        # Log the results with enhanced debugging
        if injection_success:
            print(f"✅ Anti-tracking script injected successfully via {method_used}")
            if blocked_count > 0:
                print(f"🛡️  Blocked {blocked_count} LinkedIn tracking script(s)")
            self._log_event("anti_tracking_injection", {
                "success": True,
                "method": method_used,
                "url": current_url,
                "blocked_scripts": blocked_count,
                "page_ready": page_ready
            })
        else:
            print(f"❌ Anti-tracking script injection failed")
            
            # Provide detailed debugging information
            debug_info = []
            if not page_ready:
                debug_info.append("Page not ready")
            if script_error:
                debug_info.append(f"Script error: {script_error}")
            if cdp_error:
                debug_info.append(f"CDP error: {cdp_error}")
            
            if debug_info:
                print(f"🔍 Injection failure details: {' | '.join(debug_info)}")
            
            self._log_event("anti_tracking_injection", {
                "success": False,
                "method": method_used,
                "url": current_url,
                "error": "injection_failed",
                "page_ready": page_ready,
                "script_error": script_error,
                "cdp_error": cdp_error
            })
            
        return injection_success

    def _ensure_linkedin_script_injected(self):
        """Ensure anti-tracking script is injected on current LinkedIn page."""
        try:
            # Wait for page to be ready before injection
            import time
            time.sleep(1)  # Give page time to stabilize
            
            current_url = self.driver.current_url
            if 'linkedin.com' in current_url:
                self._inject_linkedin_anti_tracking_script(self.driver)
        except Exception as e:
            print(f"⚠️  Script injection error (non-critical): {e}")
            pass

    def _detect_real_ip(self) -> dict:
        """
        Detect the real IP address before any proxy setup.
        This runs without any proxy configuration to establish baseline.
        """
        try:
            import requests
            print("🔍 Detecting real IP address (no proxy)...")
            
            # Use a reliable endpoint
            endpoint = "https://ipinfo.io/json"
            response = requests.get(endpoint, timeout=10)
            data = response.json()
            
            real_ip = data.get('ip', 'Unknown')
            city = data.get('city', '')
            region = data.get('region', '')
            country = data.get('country', 'Unknown')
            isp = data.get('org', '')
            
            location_str = f"{country}"
            if region: location_str += f", {region}"
            if city: location_str += f", {city}"
            
            return {
                'ip': real_ip,
                'country': country,
                'region': region,
                'city': city,
                'isp': isp,
                'location': location_str
            }
        except Exception as e:
            print(f"⚠️  Could not detect real IP: {e}")
            return { 'ip': 'Unknown', 'location': 'Unknown' }

    def _get_ip_with_temp_driver(self) -> str | None:
        """
        Launches a temporary, headless browser with proxy settings to get the current IP.
        Uses selenium-wire for authenticated proxies.
        """
        print("⚪ Launching temporary browser for proxy verification...")
        
        temp_driver = None
        
        if USE_DATA_IMPULSE and SELENIUM_WIRE_AVAILABLE:
            # Use selenium-wire for temp driver (proven to work)
            # SSL certificate should already be installed by main driver setup
            
            country = getattr(self, '_proxy_country', None)
            
            # Build auth username
            auth_user = DI_USERNAME
            if country:
                auth_user = f"{DI_USERNAME}__cr.{country}"
            if DI_STICKY_SESSION:
                auth_user += f";session.{DI_STICKY_SESSION}"
            
            proxy_url = f"http://{auth_user}:{DI_PASSWORD}@{DI_HOST}:{DI_PORT}"
            
            seleniumwire_options = {
                "proxy": {
                    "http": proxy_url,
                    "https": proxy_url,
                },
                # SSL options to prevent strikethrough HTTPS
                "verify_ssl": True,
                "suppress_connection_errors": True,  # Suppress ConnectionAbortedError
                "connection_keep_alive": False,  # Disable keep-alive to reduce connection errors
                "disable_encoding": True,  # Disable encoding to reduce connection issues
                "backend": "default",  # Use default backend for stability
                # Add options to handle malformed responses
                "ignore_http_methods": [],  # Capture all methods
                "request_storage": "memory",  # Use memory storage
                "request_storage_max_size": 100,  # Limit stored requests
                # Disable response body decoding to avoid gzip errors
                "disable_capture": False,
                "enable_har": False,  # Disable HAR to reduce processing
            }
            
            temp_options = wire_webdriver.ChromeOptions()
            temp_options.add_argument("--headless=new")
            
            # Disable automation detection for temp driver (Chrome 138 compatible)
            temp_options.add_argument('--disable-blink-features=AutomationControlled')
            temp_options.add_argument('--disable-infobars')
            
            # Suppress console errors and logs for temp driver (Chrome 138 compatible)
            temp_options.add_argument('--log-level=3')
            temp_options.add_argument('--disable-logging')
            temp_options.add_argument('--silent')
            temp_options.add_argument('--disable-gpu')
            temp_options.add_argument('--disable-features=VizDisplayCompositor,VoiceTranscription,OptimizationHints,TranslateUI')
            temp_options.add_argument('--disable-component-update')
            
            # Basic stability arguments
            temp_options.add_argument("--no-sandbox")
            temp_options.add_argument("--disable-dev-shm-usage")
            temp_options.add_argument("--window-size=800,600")
            
            # NO SSL-bypassing arguments - use genuine SSL verification
            # Certificate should be installed, so SSL should work properly
            
            # Keep temp driver minimal for stealth
            
            # Add retry logic for Windows selenium-wire socket issues
            max_retries = 3
            retry_delay = 2
            
            for attempt in range(max_retries):
                try:
                    # Add a small delay between attempts to avoid socket conflicts
                    if attempt > 0:
                        import time
                        time.sleep(retry_delay)
                        print(f"🔄 Retry attempt {attempt + 1}/{max_retries}...")
                    
                    temp_driver = wire_webdriver.Chrome(
                        options=temp_options,
                        seleniumwire_options=seleniumwire_options
                    )
                    print(f"✅ Created temp browser with selenium-wire (attempt {attempt + 1})")
                    break
                except Exception as e:
                    print(f"❌ Temp driver attempt {attempt + 1} failed: {e}")
                    if attempt == max_retries - 1:
                        print(f"❌ All temp driver attempts failed")
                        return None
                    else:
                        # Clean up any partial connections
                        try:
                            import psutil
                            # Kill any hanging chrome processes
                            for proc in psutil.process_iter(['pid', 'name']):
                                if 'chrome' in proc.info['name'].lower():
                                    try:
                                        proc.terminate()
                                    except:
                                        pass
                        except:
                            pass
                
        elif USE_DATA_IMPULSE:
            # Fallback to extension method
            temp_options = uc.ChromeOptions()
            temp_options.add_argument("--headless=new")
            temp_options.add_argument("--no-sandbox")
            temp_options.add_argument("--disable-dev-shm-usage")
            temp_options.add_argument("--window-size=800,600")
            temp_options.add_argument('--disable-blink-features=AutomationControlled')
            
            # Add conservative logging suppression for extension temp driver
            temp_options.add_argument('--log-level=3')
            temp_options.add_argument('--disable-logging')
            temp_options.add_argument('--silent')
            temp_options.add_argument('--disable-gpu')
            temp_options.add_argument('--disable-features=VoiceTranscription,OptimizationHints,TranslateUI')
            temp_options.add_argument('--disable-component-update')
            
            country = getattr(self, '_proxy_country', None)
                
            try:
                major_version, _ = get_chrome_main_version()
                temp_driver = uc.Chrome(options=temp_options, version_main=major_version, service_log_path='NUL' if platform.system() == 'Windows' else '/dev/null')
            except Exception as e:
                print(f"❌ Failed to create driver: {e}")
                return None
                
        elif PROXY:
            temp_options = uc.ChromeOptions()
            temp_options.add_argument("--headless=new")
            temp_options.add_argument("--no-sandbox")
            temp_options.add_argument("--disable-dev-shm-usage")
            temp_options.add_argument("--window-size=800,600")
            temp_options.add_argument(f'--proxy-server={PROXY}')
            
            # Add conservative logging suppression for simple proxy temp driver
            temp_options.add_argument('--log-level=3')
            temp_options.add_argument('--disable-logging')
            temp_options.add_argument('--silent')
            temp_options.add_argument('--disable-gpu')
            temp_options.add_argument('--disable-features=VoiceTranscription,OptimizationHints,TranslateUI')
            temp_options.add_argument('--disable-component-update')
            
            try:
                major_version, _ = get_chrome_main_version()
                temp_driver = uc.Chrome(options=temp_options, version_main=major_version, service_log_path='NUL' if platform.system() == 'Windows' else '/dev/null')
            except Exception as e:
                print(f"❌ Failed to create driver: {e}")
                return None
        else:
                return None
        
        if not temp_driver:
            return None
        
        try:
            # Using a simple, reliable endpoint
            temp_driver.get("https://ipinfo.io/ip")
            
            # Wait for the IP address to appear
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.webdriver.common.by import By
            
            ip_element = WebDriverWait(temp_driver, 60).until(  # Extended for slow proxy
                EC.presence_of_element_located((By.TAG_NAME, "pre"))
            )
            detected_ip = ip_element.text.strip()
            
            # Basic validation
            if not re.match(r"^\d{1,3}(\.\d{1,3}){3}$", detected_ip):
                return None
            
            return detected_ip
        except Exception as e:
            print(f"❌ Temporary browser verification failed: {e}")
            return None
        finally:
            if temp_driver:
                temp_driver.quit()
                print("⚪ Temporary browser closed.")
            
            # No certificate cleanup needed since we let selenium-wire handle it

    def _verify_proxy(self):
        """
        Verifies the proxy using a temporary browser. Halts if proxy fails or real IP is detected.
        """
        if not USE_DATA_IMPULSE and not PROXY:
            print("⚪ No proxy configured, skipping verification.")
            return

        detected_ip = self._get_ip_with_temp_driver()

        if not detected_ip:
            print(f"🚨 CRITICAL: Proxy verification FAILED. Could not determine browsing IP.")
            print(f"🛑 HALTING EXECUTION to protect your real IP address.")
            os._exit(1)

        if detected_ip == self._real_ip_info['ip']:
            print(f"🚨 CRITICAL: Real IP detected! Your location is EXPOSED!")
            print(f"   IP: {detected_ip}")
            print(f"   Location: {self._real_ip_info.get('location', 'N/A')}")
            print(f"   ISP: {self._real_ip_info.get('isp', 'N/A')}")
            print(f"🛑 HALTING EXECUTION - Proxy is not working!")
            os._exit(1)

        print(f"✅ Proxy is ACTIVE and working correctly.")
        print(f"   Browsing IP: {detected_ip}")
        
        # Now, get location for the PROXY ip
        try:
            geo_response = requests.get(f"https://ipwho.is/{detected_ip}", timeout=10)
            geo_data = geo_response.json()
            country = geo_data.get('country', 'Unknown')
            region = geo_data.get('region', '')
            city = geo_data.get('city', '')
            isp = geo_data.get('connection', {}).get('isp', '')
            
            location_str = f"{country}"
            if region: location_str += f", {region}"
            if city: location_str += f", {city}"
            
            print(f"   Proxy Location: {location_str}")
            if isp: print(f"   Proxy ISP: {isp}")
            self._log_event("proxy_verify", {"ip": detected_ip, "location": location_str, "isp": isp})
        except Exception as e:
            print(f"   (Could not fetch location details for proxy IP: {e})")
            self._log_event("proxy_verify", {"ip": detected_ip})

    # ---------- auth ----------
    def login(self):
        # If already in feed, we're logged in – skip prompts.
        self.driver.get("https://www.linkedin.com/feed/")
        self._ensure_linkedin_script_injected()
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
        if not "linkedin.com/feed" in self.driver.current_url:
            self.driver.get("https://www.linkedin.com/login")
            self._ensure_linkedin_script_injected()
        try:
            WebDriverWait(self.driver, 300).until(EC.url_contains("/feed"))  # Extended for slow proxy
        except Exception:
            # Try submit with env creds if provided (optional convenience)
            if LINKEDIN_USERNAME and LINKEDIN_PASSWORD:
                try:
                    user = WebDriverWait(self.driver, 30).until(  # Extended for slow proxy
                        EC.presence_of_element_located(("css selector", "input#username, input[name='session_key']"))
                    )
                    pwd = self.driver.find_element("css selector", "input#password, input[name='session_password']")
                    user.clear()
                    self._human_type(user, LINKEDIN_USERNAME)
                    self._human_delay(0.5, 1.0)
                    pwd.clear()
                    self._human_type(pwd, LINKEDIN_PASSWORD)
                    self._human_delay(0.3, 0.8)
                    try:
                        pwd.send_keys(Keys.RETURN)
                    except Exception:
                        pass
                    WebDriverWait(self.driver, 300).until(EC.url_contains("/feed"))  # Extended for slow proxy
                except Exception:
                    pass  # fall through

        if "linkedin.com/feed" not in self.driver.current_url:
            raise RuntimeError("Login not completed. Please sign in within 3 minutes and try again.")

        # Persist cookies for next run
        save_cached_cookies(self.driver.get_cookies())
    
    def _force_click(self, el):
        """Click element robustly using only trusted event methods."""
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
        
        # Try ActionChains with explicit move
        try:
            ActionChains(self.driver).move_to_element(el).pause(0.1).click().perform()
            return
        except Exception:
            pass
        
        # Try clicking at element center
        try:
            # Get element center coordinates
            location = el.location
            size = el.size
            center_x = location['x'] + size['width'] // 2
            center_y = location['y'] + size['height'] // 2
            
            # Move to center and click
            ActionChains(self.driver).move_by_offset(center_x, center_y).click().perform()
            return
        except Exception:
            pass
        
        # Final attempt: scroll into view, wait, then ActionChains
        try:
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'center'});", el)
            self._human_delay(0.3, 0.5)
            ActionChains(self.driver).move_to_element(el).click().perform()
        except Exception as e:
            print(f"Warning: Could not click element with trusted events: {e}")

    def _full_login(self):
        user_box = self.wait.until(EC.element_to_be_clickable((S.SEARCH_BOX[0], 'input[name="session_key"]')))
        self._human_type(user_box, LINKEDIN_USERNAME)
        pwd_box = self.driver.find_element("name", "session_password")
        self._human_type(pwd_box, LINKEDIN_PASSWORD)
        self._human_delay(0.2, 0.6)
        pwd_box.send_keys(Keys.RETURN)
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
    def _clear_company_filter(self):
        """
        Clear any previous company selections in the current company filter.
        This ensures that when searching for a new school, the previous company
        selection doesn't interfere with the results.
        """
        try:
            # Check if we're on a search results page that might have company filters
            current_url = self.driver.current_url
            if "search/results/people" not in current_url:
                # Not on a people search page, no need to clear
                return
            
            # First, check if there's an active company filter to clear
            try:
                company_pill = self.driver.find_element(By.ID, "searchFilter_currentCompany")
                pill_text = company_pill.get_attribute("textContent") or ""
                aria_expanded = company_pill.get_attribute("aria-expanded") or "false"
                
                # Check if the pill shows indicators of selection
                has_active_filter = any(indicator in pill_text.lower() for indicator in ["•", "1", "2", "3", "4", "5", "6", "7", "8", "9"])
                
                if not has_active_filter:
                    print("ℹ️  No active company filters detected")
                    return
                    
                print("🧹 Active company filter detected, opening dropdown to find reset button...")
                
                # The reset button only appears when the dropdown is open, so we need to click the pill first
                if aria_expanded == "false":
                    self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", company_pill)
                    self._human_delay(0.1, 0.3)
                    self._human_click(company_pill)
                    self._human_delay(0.5, 1.0)  # Wait for dropdown to open
                
                # Now look for the reset button in the opened dropdown
                reset_button = None
                
                # Strategy 1: Look for Spanish "Restablecer" button
                try:
                    reset_button = self.driver.find_element(
                        By.XPATH,
                        "//button[contains(@aria-label, 'Restablecer el filtro') and contains(@aria-label, 'Empresa actual')]"
                    )
                except Exception:
                    pass
                
                # Strategy 2: Look for English "Reset" button if Spanish not found
                if not reset_button:
                    try:
                        reset_button = self.driver.find_element(
                            By.XPATH,
                            "//button[contains(@aria-label, 'Reset') and contains(@aria-label, 'Current company')]"
                        )
                    except Exception:
                        pass
                
                # Strategy 3: Look for button with "Restablecer" or "Reset" text within the dropdown
                if not reset_button:
                    try:
                        reset_button = self.driver.find_element(
                            By.XPATH,
                            "//div[contains(@class, 'reusable-search-filters-trigger-dropdown__content')]//button[.//span[contains(text(), 'Restablecer') or contains(text(), 'Reset')]]"
                        )
                    except Exception:
                        pass
                
                # Strategy 4: Look for more generic reset button patterns within the dropdown
                if not reset_button:
                    try:
                        reset_button = self.driver.find_element(
                            By.XPATH,
                            "//div[@aria-hidden='false']//button[contains(@class, 'artdeco-button--muted') and (.//span[contains(text(), 'Restablecer') or contains(text(), 'Reset')])]"
                        )
                    except Exception:
                        pass
                
                if reset_button:
                    print("✅ Found company filter reset button, clearing previous selection...")
                    self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", reset_button)
                    self._human_delay(0.1, 0.3)
                    self._human_click(reset_button)
                    self._human_delay(0.5, 1.0)
                    print("✅ Company filter reset successfully")
                    return
                else:
                    print("⚠️  Company filter appears active but no reset button found in dropdown")
                    # Try clicking the pill again to close the dropdown
                    try:
                        self._human_click(company_pill)
                        self._human_delay(0.3, 0.5)
                    except Exception:
                        pass
                        
            except Exception as e:
                print(f"⚠️  Error finding company filter pill: {e}")
                pass
            
        except Exception as e:
            print(f"⚠️  Error while clearing company filter: {e}")
            # Don't fail the entire search if filter clearing fails
            pass

    def search_school(self, school_name: str):
        """
        Search for a school. This function now attempts to reuse the existing
        search bar on the page before navigating back to the feed, making the
        interaction more human-like.
        """
        try:
            # Strategy 1: Find search box on the current page (less disruptive)
            box = self.driver.find_element(*S.SEARCH_BOX)
        except Exception:
            # Strategy 2: Fallback to navigating to the feed
            try:
                self.driver.get("https://www.linkedin.com/feed/")
                self._ensure_linkedin_script_injected()
                self._human_delay(0.8, 1.6)
                box = self.wait.until(EC.element_to_be_clickable(S.SEARCH_BOX))
            except Exception:
                # Strategy 3: Hard refresh as a last resort
                self.driver.refresh()
                self._human_delay(0.8, 1.6)
                box = self.wait.until(EC.element_to_be_clickable(S.SEARCH_BOX))

        # First, clear any previous company selections before searching
        self._clear_company_filter()

        box.clear()
        self._human_type(box, school_name)
        self._human_delay(0.5, 1.0)
        box.send_keys(Keys.RETURN)
        
        # Wait for search results to load and check if People filter is already selected
        self._human_delay(1.0, 2.0)
        
        # Only click People pill if it's not already selected
        try:
            people_pill = self.wait.until(EC.element_to_be_clickable(S.PILL_PEOPLE))
            # Check if the pill is already selected (usually has aria-pressed="true" or selected class)
            is_selected = (
                people_pill.get_attribute("aria-pressed") == "true" or
                "selected" in (people_pill.get_attribute("class") or "") or
                "active" in (people_pill.get_attribute("class") or "")
            )
            
            if not is_selected:
                print("🔄 People filter not selected, clicking...")
                self._move_and_click(people_pill)
                self._human_delay()
                # Wait for toolbar re-render to avoid stale elements
                try:
                    filters_bar = self.driver.find_element(By.ID, "search-reusables__filters-bar")
                    WebDriverWait(self.driver, 5).until(EC.staleness_of(filters_bar))
                except Exception:
                    pass
            else:
                print("ℹ️  People filter already selected, skipping click")
        except Exception as e:
            print(f"⚠️  Could not check/click People filter: {e}")

        # ---------------- helpers ----------------
        def _best_label_in_items(container, school, min_score=80):
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

            # Scope strictly to the currently visible currentCompany dropdown
            try:
                scoped_container = self.driver.find_element(
                    By.CSS_SELECTOR,
                    "div.search-reusables__filter-trigger-and-dropdown[data-basic-filter-parameter-name='currentCompany'] "
                    "div.reusable-search-filters-trigger-dropdown__content[aria-hidden='false']"
                )
            except Exception:
                scoped_container = container

            items = scoped_container.find_elements(By.CSS_SELECTOR, "li.search-reusables__collection-values-item")
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
            
            # Return tuple (label, score) only if it meets the minimum score threshold
            if best_match_label is None or highest_score < min_score:
                return None
            return best_match_label, highest_score

        def _open_current_company_container():
            """
            Find the pill, get its aria-controls, and return (pill, container).
            Use a minimal, reliable open that doesn't toggle the dropdown closed.
            """
            # Find pill element
            pill = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "searchFilter_currentCompany"))
            )
            try:
                self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", pill)
            except Exception:
                pass

            # Humanized open: hover, pause, click; fallbacks to element.click and finally JS
            open_started = time.time()
            method_used = "actionchains"
            try:
                ActionChains(self.driver).move_to_element(pill).pause(random.uniform(0.08, 0.25)).click(pill).perform()
            except Exception:
                try:
                    method_used = "element.click"
                    pill.click()
                except Exception:
                    try:
                        method_used = "force_click"
                        self._force_click(pill)
                    except Exception as e:
                        print(e)
                        pass

            # Wait for visible container
            try:
                container = WebDriverWait(self.driver, 6).until(
                    EC.presence_of_element_located((
                        By.CSS_SELECTOR,
                        "div.search-reusables__filter-trigger-and-dropdown[data-basic-filter-parameter-name='currentCompany'] "
                        "div.reusable-search-filters-trigger-dropdown__content[aria-hidden='false']"
                    ))
                )
            except Exception:
                container = None

            # Log open duration + method
            try:
                self._log_event("company_dropdown_open", {
                    "elapsed_ms": int(1000*(time.time()-open_started)),
                    "success": bool(container),
                    "method": method_used,
                })
            except Exception:
                pass
            return pill, container

        def _apply_selection_in_container(container, school):
            """Click label (via JS if necessary) and then click 'Show results' inside same container."""
            # Always re-resolve to the visible container for the currentCompany filter to avoid matching other filters
            try:
                visible_scoped = self.driver.find_element(
                    By.CSS_SELECTOR,
                    "div.search-reusables__filter-trigger-and-dropdown[data-basic-filter-parameter-name='currentCompany'] "
                    "div.reusable-search-filters-trigger-dropdown__content[aria-hidden='false']"
                )
                container = visible_scoped
            except Exception:
                pass
            # Pick the best label and enforce a threshold
            select_started = time.time()
            result = _best_label_in_items(container, school, min_score=80)
            selection_elapsed_ms = int(1000*(time.time()-select_started))
            if not result:
                try:
                    self._log_event("company_selection", {"selected": False, "reason": "no_items", "elapsed_ms": selection_elapsed_ms})
                except Exception:
                    pass
                # Raise NoGoodMatchFound immediately when no companies are found at all
                raise NoGoodMatchFound(f"No companies found in dropdown for '{school}'")
            label, best_score = result
            MIN_ACCEPTABLE = 80
            if best_score < MIN_ACCEPTABLE:
                print(f"No acceptable match (best={best_score}) for '{school}' — skipping selection.")
                try:
                    self._log_event("company_selection", {"selected": False, "reason": "low_score", "best_score": best_score, "elapsed_ms": selection_elapsed_ms})
                except Exception:
                    pass
                # Raise NoGoodMatchFound immediately for low similarity scores
                raise NoGoodMatchFound(f"No acceptable company match for '{school}' (best similarity: {best_score:.1f}%, required: {MIN_ACCEPTABLE}%)")

            # Ensure dropdown is actually open/visible before selecting
            try:
                aria = (container.get_attribute("aria-hidden") or "").strip().lower()
                klass = container.get_attribute("class") or ""
                if not (aria == "false" or "artdeco-hoverable-content--visible" in klass or container.is_displayed()):
                    try:
                        self._log_event("company_selection", {"selected": False, "reason": "not_visible"})
                    except Exception:
                        pass
                    return False
            except Exception:
                return False

            # Select checkbox by clicking the <label> using trusted events
            try:
                self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", label)
                self._human_delay(0.05, 0.15)
                click_start = time.time()
                # Use _human_click for trusted events
                self._human_click(label)
                try:
                    self._log_event("company_selection", {"selected": True, "best_score": best_score, "click_elapsed_ms": int(1000*(time.time()-click_start))})
                except Exception:
                    pass
            except Exception:
                return False

            # Click Show results in THIS container
            try:
                # Multiple fallback strategies for finding the show results button
                show_btn = None
                
                # Strategy 1: English aria-label
                try:
                    show_btn = container.find_element(By.CSS_SELECTOR, "button[aria-label='Apply current filter to show results']")
                except Exception:
                    pass
                
                # Strategy 2: Spanish aria-label (most specific)
                if not show_btn:
                    try:
                        show_btn = container.find_element(By.CSS_SELECTOR, "button[aria-label='Aplicar el filtro actual para mostrar resultados']")
                    except Exception:
                        pass
                
                # Strategy 3: Partial aria-label match (flexible)
                if not show_btn:
                    try:
                        show_btn = container.find_element(By.XPATH, ".//button[contains(@aria-label, 'mostrar resultados') or contains(@aria-label, 'Show results') or contains(@aria-label, 'Apply')]")
                    except Exception:
                        pass
                
                # Strategy 4: Text content match with primary button class
                if not show_btn:
                    try:
                        show_btn = container.find_element(By.XPATH, ".//button[contains(@class, 'artdeco-button--primary') and (.//span[contains(text(), 'Mostrar resultados') or contains(text(), 'Show results')])]")
                    except Exception:
                        pass
                
                # Strategy 5: Broader fallback based on button structure and classes
                if not show_btn:
                    try:
                        show_btn = WebDriverWait(container, 8).until(
                            EC.presence_of_element_located((
                                By.XPATH,
                                    ".//button[contains(@class, 'artdeco-button--2') and contains(@class, 'artdeco-button--primary') and .//span[@class='artdeco-button__text']]"
                            ))
                        )
                    except Exception:
                        pass
                
                if not show_btn:
                    raise Exception("Could not find show results button with any strategy")
                self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", show_btn)
                self._human_delay(0.05, 0.15)
                click_sr_start = time.time()
                # Use _human_click for trusted events
                self._human_click(show_btn)
                show_results_elapsed = int(1000*(time.time()-click_sr_start))

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
                try:
                    self._log_event("company_show_results", {"click_elapsed_ms": show_results_elapsed})
                except Exception:
                    pass
                return True
            except Exception:
                return False

        # NOTE: Hard JS fallback temporarily disabled per request
        def _js_open_select_and_apply(school: str) -> bool:  # disabled
            """Hard JS fallback disabled to avoid selecting without dropdown visibly open."""

        # ---------------- Strategy A: try the pill container ----------------
        try:
            pill, container = _open_current_company_container()
            if container and _apply_selection_in_container(container, school_name):
                return
        except NoGoodMatchFound:
            # Re-raise our specific exception so it gets properly handled by main.py
            raise
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
            
            # ---------------- Strategy C: Hard JS fallback (disabled) ----------------
            # Intentionally disabled to avoid selecting without dropdown open

            # If all strategies fail, raise our specific exception.
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

        def get_container():
            try:
                if controls_id:
                    return self.driver.find_element(By.ID, controls_id)
            except Exception:
                pass
            try:
                outlet = self.driver.find_element(By.ID, "hoverable-outlet-current-company-filter-value")
                return outlet.find_element(
                    By.XPATH,
                    ".//div[contains(@class,'artdeco-hoverable-content') and @role='tooltip']"
                )
            except Exception:
                return None

        def is_visible(cont_el: WebElement | None) -> bool:
            if not cont_el:
                return False
            try:
                aria = (cont_el.get_attribute("aria-hidden") or "").strip().lower()
                klass = cont_el.get_attribute("class") or ""
                return aria == "false" or ("artdeco-hoverable-content--visible" in klass)
            except Exception:
                return False

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
                self._human_delay(0.05, 0.15)
            except Exception:
                pass

            self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            self._human_delay(0.05, 0.15)

            # try: hover + click; hover + double-click; JS click fallback; Enter key
            try:
                ActionChains(self.driver).move_to_element(btn).pause(random.uniform(0.1, 0.3)).perform()
                self._human_delay(0.05, 0.15)
                self._human_click(btn)
            except Exception:
                try:
                    ActionChains(self.driver).move_to_element(btn).pause(0.05).double_click(btn).perform()
                except Exception:
                    try:
                        self._force_click(btn)
                    except Exception:
                        pass
            # Enter key as alternate toggle
            try:
                btn.send_keys(Keys.ENTER)
            except Exception:
                pass

            try:
                WebDriverWait(self.driver, 3.0).until(
                    lambda d: is_open(d) or is_visible(get_container()) or (btn.get_attribute('aria-expanded') or '').lower() == 'true'
                )
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
            # 1. Perform a few human-like scrolls first, then ensure absolute bottom
            try:
                self._human_scroll_down(num_scrolls=random.randint(3, 6), delay_between=0.2)
            except Exception:
                pass
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            self._human_delay(0.1, 0.3)

            # 2. Define a specific locator to avoid finding hidden duplicates.
            locator = (By.CLASS_NAME, "artdeco-pagination")

            # 3. Explicitly wait for the element to become VISIBLE. This is the
            # crucial step that solves the race condition. The script will poll
            # the DOM for up to 12 seconds until the element appears.
            state_el = WebDriverWait(self.driver, 12).until(
                EC.visibility_of_element_located(locator)
            )

            # 4. Once visible, get the text and parse it (bilingual support)
            txt = state_el.text.strip()
            
            # Try English pattern first
            m = re.search(r"Page\s+(\d+)\s+of\s+(\d+)", txt)
            
            # If English failed, try Spanish pattern
            if not m:
                m = re.search(r"Página\s+(\d+)\s+de\s+(\d+)", txt)
            
            # Extract page numbers from the raw text as fallback
            if not m:
                # Extract all numbers from the text as a fallback
                numbers = re.findall(r'\d+', txt)
                if len(numbers) >= 2:
                    # In pagination like "Página 1 de 18\nAnterior\n1\n2\n3...", 
                    # the first two numbers are usually current page and total pages
                    return int(numbers[0]), int(numbers[1])
                else:
                    # This error is a safeguard in case the element is visible but unparseable
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
        self._human_delay(0.05, 0.15)
        try:
            self._human_click(next_btn)
        except Exception:
            try:
                self._force_click(next_btn)
            except Exception:
                pass

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
            WebDriverWait(self.driver, 45).until(page_or_results_changed)  # Extended for slow proxy
            self._human_delay(0.8, 1.6)
            return True
        except TimeoutException:
            print("⚠️ Next click didn't change results; staying on this page.")
            return False

    def harvest_profiles(self, school_name: str) -> Generator[Dict[str, Any], None, None]:
        """
        Harvest all pages, yielding each profile contact as it's processed.
        This allows for iterative saving and is crash-safe.
        """
        cur, last = self._get_page_numbers()
        
        innocent_action_counter = 0
        
        while True:
            # Yield each contact from the current page
            for contact in self._harvest_current_page(school_name):
                self._profiles_processed += 1
                
                # Periodic proxy verification every 20 profiles
                if self._profiles_processed % self._proxy_check_interval == 0:
                    print(f"🔄 Proxy check #{self._profiles_processed // self._proxy_check_interval} (after {self._profiles_processed} profiles)")
                    self._verify_proxy()
                
                yield contact
            
            # Check if we are on the last page
            try:
                cur, last = self._get_page_numbers()
            except Exception:
                cur += 1 # Failsafe if pagination disappears
            
            if cur >= last:
                break
                
            if not self._click_next_page(cur):
                print("ℹ️ Could not advance to next page (maybe last page?).")
                break


            
            innocent_action_counter += 1
            if innocent_action_counter >= random.randint(2, 4):
                self._perform_innocent_action()
                innocent_action_counter = 0


    def _harvest_current_page(self, school_name: str) -> Generator[Dict[str, Any], None, None]:
        """
        Open each profile link, extract data, and yield the contact.
        This is a generator function to support iterative processing.
        """
        print("Harvesting current page")
        
        # Human-like scrolling pattern to reach bottom naturally
        try:
            # Get current scroll position and total height
            current_scroll = self.driver.execute_script("return window.pageYOffset;")
            total_height = self.driver.execute_script("return document.body.scrollHeight;")
            viewport_height = self.driver.execute_script("return window.innerHeight;")
            
            # Scroll in partial increments like a human would
            while current_scroll < (total_height - viewport_height):
                # Vary scroll distance (30-70% of viewport)
                scroll_distance = random.uniform(0.3, 0.7) * viewport_height
                
                # Sometimes over-scroll and come back up
                if random.random() < 0.2:
                    scroll_distance *= 1.2
                
                target_scroll = min(current_scroll + scroll_distance, total_height - viewport_height)
                
                # Smooth scroll to target
                self.driver.execute_script(f"window.scrollTo({{top: {target_scroll}, behavior: 'smooth'}});")
                
                # Variable pause between scrolls (reading/scanning)
                self._human_delay(0.3, 1.2)
                
                # Update current position
                current_scroll = self.driver.execute_script("return window.pageYOffset;")
                
                # Sometimes pause longer (as if reading something interesting)
                if random.random() < 0.3:
                    self._human_delay(1.0, 2.5)
                
                # Occasionally scroll back up a bit
                if random.random() < 0.15:
                    back_scroll = random.uniform(0.1, 0.3) * viewport_height
                    self.driver.execute_script(f"window.scrollBy(0, -{back_scroll});")
                    self._human_delay(0.5, 1.0)
                    current_scroll = self.driver.execute_script("return window.pageYOffset;")
            
            # Final adjustment to ensure we're at bottom
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            self._human_delay(0.6, 1.2)
            
        except Exception:
            # Fallback to simple scrolling
            self._human_scroll_down(num_scrolls=random.randint(3, 6), delay_between=0.3)
        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        self._human_delay(0.6, 1.2)

        # Collect result card containers and main profile anchor within each
        cards = self.driver.find_elements(
            By.XPATH,
            "//div[@data-chameleon-result-urn and contains(@data-view-name,'search-entity-result')]"
        )
        card_anchors: list[tuple[WebElement, WebElement, str]] = []
        for card in cards:
            try:
                anchor = card.find_element(
                    By.XPATH,
                    ".//div[contains(@class,'mb1')]//a[contains(@href,'/in/') and not(ancestor::div[contains(@class,'entity-result__insights')])]"
                )
                href = (anchor.get_attribute("href") or "").strip()
                if not href:
                    continue
                card_anchors.append((card, anchor, href))
            except Exception:
                    continue

        # Fallback to link-only collection if no anchors found
        if not card_anchors:
            hrefs = self._collect_profile_links()
            
            # Check if this is the first page and has no profiles - skip school if so
            try:
                current_page, total_pages = self._get_page_numbers()
                if current_page == 1 and len(hrefs) == 0:
                    print(f"🟡 No profile links found on first page for this school - skipping remaining pagination")
                    raise NoGoodMatchFound(f"No accessible profiles found on first page for '{school_name}' - likely no connections available")
            except Exception:
                # If we can't get page numbers, still check for empty first page
                # Assume it's first page if we haven't yielded any profiles yet
                if len(hrefs) == 0:
                    print(f"🟡 No profile links found on what appears to be the first page - skipping school")
                    raise NoGoodMatchFound(f"No accessible profiles found for '{school_name}' - likely no connections available")
            
            for idx, href in enumerate(hrefs, 1):
                try:
                    print(f"  → [{idx}/{len(hrefs)}] Opening {href}")
                    self._human_delay(0.25, 0.7)
                    profile_link_element = self.driver.find_element(By.XPATH, f"//a[@href='{href}']")
                    self._open_in_new_tab_human_like(profile_link_element)
                    self.driver.switch_to.window(self.driver.window_handles[-1])
                    self._human_delay(0.2, 0.6)
                    # Wait for profile-card to ensure page is fully loaded before extraction
                    try:
                        WebDriverWait(self.driver, 25).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "section.artdeco-card.pv-profile-card"))
                        )
                        # Inject anti-tracking script on the profile page
                        self._ensure_linkedin_script_injected()
                    except TimeoutException:
                        # Fallback to main text if profile-card not found
                        WebDriverWait(self.driver, 20).until(EC.presence_of_element_located(S.MAIN_TEXT))
                        # Still try to inject anti-tracking script even if profile card not found
                        self._ensure_linkedin_script_injected()
                    contact = self._extract_profile_current_tab(school_name, href)
                    yield contact
                except Exception:
                    print(f"    ⚠️  Skipping profile due to error:\n{traceback.format_exc()}")
                finally:
                    try:
                        if len(self.driver.window_handles) > 1:
                            self.driver.close()
                            self.driver.switch_to.window(self.driver.window_handles[0])
                            self._human_delay(0.15, 0.35)
                    except Exception:
                        pass
            print(f"✅ Finished harvesting this page")
            return

        # Check if this is the first page and has no profiles - skip school if so
        try:
            current_page, total_pages = self._get_page_numbers()
            if current_page == 1 and len(card_anchors) == 0:
                print(f"🟡 No profile links found on first page for this school - skipping remaining pagination")
                raise NoGoodMatchFound(f"No accessible profiles found on first page for '{school_name}' - likely no connections available")
        except Exception:
            # If we can't get page numbers, still check for empty first page
            # Assume it's first page if we haven't yielded any profiles yet
            if len(card_anchors) == 0:
                print(f"🟡 No profile links found on what appears to be the first page - skipping school")
                raise NoGoodMatchFound(f"No accessible profiles found for '{school_name}' - likely no connections available")

        # Prefer human-like interaction with visible anchors
        for idx, (card, anchor, href) in enumerate(card_anchors, 1):
            try:
                profile_name = (anchor.text or "").strip()
                print(f"  → [{idx}/{len(card_anchors)}] Opening {href}")
                self._human_delay(0.25, 0.7)
                # Hover the card slightly before opening
                try:
                    ActionChains(self.driver).move_to_element(card).pause(random.uniform(0.08, 0.2)).perform()
                except Exception:
                    pass
                open_start = time.time()
                self._open_in_new_tab_human_like(anchor)
                open_elapsed = int(1000*(time.time()-open_start))
                try:
                    self._log_event("open_profile", {"name": profile_name, "elapsed_ms": open_elapsed})
                except Exception:
                    pass
                self.driver.switch_to.window(self.driver.window_handles[-1])
                self._human_delay(0.2, 0.6)
                WebDriverWait(self.driver, 20).until(EC.presence_of_element_located(S.MAIN_TEXT))
                # Inject anti-tracking script on the profile page
                self._ensure_linkedin_script_injected()
                contact = self._extract_profile_current_tab(school_name, href)
                yield contact
            except Exception:
                print(f"    ⚠️  Skipping profile due to error:\n{traceback.format_exc()}")
            finally:
                try:
                    if len(self.driver.window_handles) > 1:
                        self.driver.close()
                        self.driver.switch_to.window(self.driver.window_handles[0])
                        self._human_delay(0.15, 0.35)
                except Exception:
                    pass

        print(f"✅ Finished harvesting this page")
    
    def _extract_profile_current_tab(self, school_name: str, href: str) -> Dict[str, Any]:
        """Assumes we're already on a profile tab. Extracts text, opens contact modal, calls OpenAI, persists."""
        # Start timer for total profile time
        profile_start_time = time.time()
        
        # Wait for profile page to fully load by checking for the profile-card section
        # This ensures all profile content (experience, skills, etc.) has loaded before scraping
        try:
            print("    ⏳ Waiting for profile page to fully load...")
            profile_card = WebDriverWait(self.driver, 25).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "section.artdeco-card.pv-profile-card"))
            )
            print("    ✅ Profile page fully loaded")
            
            # Inject anti-tracking script on the profile page
            self._ensure_linkedin_script_injected()
            
            # Additional wait for dynamic content within the profile card
            self._human_delay(1.0, 2.5)
            
        except TimeoutException:
            print("    ⚠️  Profile card not found - page may not be fully loaded, continuing anyway...")
            # Still try to inject anti-tracking script even if profile card not found
            self._ensure_linkedin_script_injected()
        except Exception as e:
            print(f"    ⚠️  Error waiting for profile card: {e}")
            # Still try to inject anti-tracking script even on error
            self._ensure_linkedin_script_injected()
        
        # 1) Main profile text
        main_text = ""
        try:
            main_text = self.driver.find_element(*S.MAIN_TEXT).text
            
            # Human-like reading time based on profile content
            # Base reading time: 5-15 seconds
            base_read_time = random.uniform(5.0, 15.0)
            
            # Add time based on text length (avg reading speed ~200-250 words/min)
            word_count = len(main_text.split())
            additional_read_time = (word_count / 250) * 60  # Convert to seconds
            
            # Cap additional time at 30 seconds
            additional_read_time = min(additional_read_time, 30.0)
            
            # Add some randomness
            total_profile_read_time = base_read_time + additional_read_time * random.uniform(0.7, 1.3)
            
            # Sometimes scroll while reading profile
            if random.random() < 0.4:
                self._human_scroll_down(num_scrolls=random.randint(1, 3), delay_between=random.uniform(0.5, 1.5))
            
            # Apply reading delay
            self._human_delay(total_profile_read_time * 0.5, total_profile_read_time * 0.7)
            
        except Exception as e:
            print(f"    ⚠️  Could not read main profile text: {repr(e)}")

        # 2) Contact info modal (randomized ~5% skip)
        contact_text = ""
        try:
            open_contact = random.random() >= 0.05
            if not open_contact:
                print("    ℹ️ Skipping contact modal for realism (5% policy).")
                raise TimeoutException("Skipping contact modal by policy")
            btn = WebDriverWait(self.driver, 15).until(EC.element_to_be_clickable(S.CONTACT_INFO_BTN))
            try:
                self._move_and_click(btn)
            except Exception:
                self._force_click(btn)
            self._human_delay(0.9, 1.5)

            modal = WebDriverWait(self.driver, 20).until(EC.visibility_of_element_located(S.CONTACT_MODAL))
            body = WebDriverWait(modal, 20).until(EC.presence_of_element_located(S.CONTACT_MODAL_BODY))

            # Let content populate (poll for some text)
            t0 = time.time()
            while time.time() - t0 < 8:
                txt = (body.text or "").strip()
                if len(txt) > 10:
                    break
            self._human_delay(0.15, 0.35)
            
            # Human-like reading time for contact info
            # People typically spend 3-15 seconds reading contact details
            read_time = random.uniform(3.0, 15.0)
            
            # If there's a lot of text, increase reading time
            if len(txt) > 200:
                read_time += random.uniform(2.0, 5.0)
            
            # Sometimes people hover over elements while reading
            if random.random() < 0.3:
                try:
                    contact_elements = modal.find_elements(By.CSS_SELECTOR, "a, span, div[class*='pv']")
                    if contact_elements:
                        element_to_hover = random.choice(contact_elements[:5])
                        ActionChains(self.driver).move_to_element(element_to_hover).pause(random.uniform(0.5, 1.5)).perform()
                except Exception:
                    pass
            
            # Actual reading delay
            self._human_delay(read_time, read_time + 2.0)

            # Strip any upsell
            try:
                for upsell in modal.find_elements(*S.CONTACT_MODAL_UPSELL):
                    self.driver.execute_script("arguments[0].remove()", upsell)
            except Exception:
                pass

            contact_text = body.text
            
            # Small delay before closing (people don't instantly close after reading)
            self._human_delay(0.5, 1.5)
            
            # Close modal
            try:
                close_btn = self.driver.find_element(*S.CONTACT_MODAL_CLOSE)
                try:
                    self._move_and_click(close_btn)
                except Exception:
                    self._force_click(close_btn)
                # Wait for modal to close
                self._human_delay(0.3, 0.8)
            except Exception:
                pass

        except Exception as e:
            print(f"    (no or skipped contact modal) {repr(e)}")

        # 3) Send to OpenAI
        combined_text = (main_text or "") + "\n" + (contact_text or "")
        prompt = TEMPLATE.format_map({"school_name": school_name, "text": combined_text})

        contact_modal_opened = bool(contact_text)
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
            # Log found keys and whether contact modal was opened
            try:
                found_keys = [k for k, v in contact.items() if k not in ("linkedin_url",) and bool(v)]
                self._log_event("profile_extracted", {"found_keys": found_keys, "contact_modal_opened": contact_modal_opened})
            except Exception:
                pass
            print(f"    💾 Saved: {json.dumps(contact, ensure_ascii=False)}")
        except Exception as e:
            print(f"    ⚠️  Failed to persist: {repr(e)}")

        # Ensure minimum time spent on profile (people rarely leave in under 3 seconds)
        total_time_on_profile = time.time() - profile_start_time
        min_profile_time = random.uniform(3.0, 5.0)
        if total_time_on_profile < min_profile_time:
            remaining_time = min_profile_time - total_time_on_profile
            self._human_delay(remaining_time, remaining_time + 1.0)

        return contact


    def _process_profile(self, url: str, school_name: str) -> Dict[str, Any]:
        """
        Open profile in a new tab, scrape main text + contact modal, call OpenAI,
        persist result to disk, close tab, and return to results.
        """
        parent = self.driver.current_window_handle
        profile_link_element = self.driver.find_element(By.XPATH, f"//a[@href='{url}']")
        self._open_in_new_tab_human_like(profile_link_element)

        self.driver.switch_to.window(self.driver.window_handles[-1])

        try:
            # Wait for profile page to fully load by checking for the profile-card section
            # This ensures all profile content (experience, skills, etc.) has loaded before scraping
            try:
                print("    ⏳ Waiting for profile page to fully load...")
                profile_card = WebDriverWait(self.driver, 25).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "section.artdeco-card.pv-profile-card"))
                )
                print("    ✅ Profile page fully loaded")
                
                # Inject anti-tracking script on the profile page
                self._ensure_linkedin_script_injected()
                
                # Additional wait for dynamic content within the profile card
                self._human_delay(1.0, 2.5)
                
            except TimeoutException:
                print("    ⚠️  Profile card not found - page may not be fully loaded, continuing anyway...")
                # Still try to inject anti-tracking script even if profile card not found
                self._ensure_linkedin_script_injected()
            except Exception as e:
                print(f"    ⚠️  Error waiting for profile card: {e}")
                # Still try to inject anti-tracking script even on error
                self._ensure_linkedin_script_injected()
            
            WebDriverWait(self.driver, 20).until(EC.presence_of_element_located(S.MAIN_TEXT))
            main_text = self.driver.find_element(*S.MAIN_TEXT).text

            # Contact modal with robust waits, randomized ~5% skip
            contact_text = ""
            try:
                open_contact = random.random() >= 0.05
                if open_contact:
                    btn = WebDriverWait(self.driver, 8).until(EC.element_to_be_clickable(S.CONTACT_INFO_BTN))
                    try:
                        self._move_and_click(btn)
                    except Exception:
                        self._force_click(btn)

                    modal = WebDriverWait(self.driver, 15).until(EC.visibility_of_element_located(S.CONTACT_MODAL))
                    body = WebDriverWait(modal, 15).until(EC.presence_of_element_located(S.CONTACT_MODAL_BODY))

                    # allow content to populate
                    t0 = time.time()
                    while time.time() - t0 < 5 and len((body.text or "").strip()) < 20:
                        self._human_delay(0.15, 0.35)

                    # Strip upsell if present
                    try:
                        for upsell in modal.find_elements(*S.CONTACT_MODAL_UPSELL):
                            self.driver.execute_script("arguments[0].remove()", upsell)
                    except Exception:
                        pass

                        contact_text = body.text
                else:
                    print("    ℹ️ Skipping contact modal for realism (5% policy).")
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
                        try:
                            self._move_and_click(btn)
                        except Exception:
                            self._force_click(btn)
                        self._human_delay(0.15, 0.35)
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

        # Human-like scrolling pattern to reach bottom naturally
        try:
            # Get current scroll position and total height
            current_scroll = self.driver.execute_script("return window.pageYOffset;")
            total_height = self.driver.execute_script("return document.body.scrollHeight;")
            viewport_height = self.driver.execute_script("return window.innerHeight;")
            
            # Scroll in partial increments
            while current_scroll < (total_height - viewport_height):
                # Vary scroll distance (40-80% of viewport)
                scroll_distance = random.uniform(0.4, 0.8) * viewport_height
                target_scroll = min(current_scroll + scroll_distance, total_height - viewport_height)
                
                # Smooth scroll
                self.driver.execute_script(f"window.scrollTo({{top: {target_scroll}, behavior: 'smooth'}});")
                self._human_delay(0.2, 0.8)
                
                # Update position
                current_scroll = self.driver.execute_script("return window.pageYOffset;")
                
                # Occasional pause (as if scanning results)
                if random.random() < 0.25:
                    self._human_delay(0.5, 1.5)
            
            # Ensure we're at bottom
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            self._human_delay(0.6, 1.2)
            
        except Exception:
            # Fallback
            self._human_scroll_down(num_scrolls=random.randint(2, 5), delay_between=0.2)
        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        self._human_delay(0.6, 1.2)

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
